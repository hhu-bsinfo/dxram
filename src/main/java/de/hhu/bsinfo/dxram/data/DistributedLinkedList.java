package de.hhu.bsinfo.dxram.data;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.ServiceProvider;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.data.holder.DistributableValue;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class DistributedLinkedList<T> implements Iterable<T> {

    private static final int LOCK_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(1);
    private static final int LOOKUP_TIMEOUT = (int) TimeUnit.MILLISECONDS.toMillis(500);

    private final MetaData metaData;
    private final DataHolder<T> current;
    private final DataHolder<T> previous;

    private final ChunkService chunkService;

    private DistributedLinkedList(final MetaData metaData, final ChunkService chunkService, final Supplier<DistributableValue<T>> valueSupplier) {
        this.metaData = metaData;
        this.chunkService = chunkService;
        current = new DataHolder<>(valueSupplier.get());
        previous = new DataHolder<>(valueSupplier.get());
    }

    public void add(T value, short nodeId) {
        syncedWrite(() -> {
            current.reset();
            current.setValue(value);
            current.setNext(ChunkID.INVALID_ID);
            chunkService.create().create(nodeId, current);

            if (metaData.head == ChunkID.INVALID_ID) {
                metaData.head = current.getID();
                metaData.tail = metaData.head;
            } else {
                previous.reset();
                previous.setID(metaData.tail);
                chunkService.get().get(previous);
                previous.setNext(current.getID());
                chunkService.put().put(previous);
                metaData.tail = current.getID();
            }

            chunkService.put().put(current);
            metaData.size += 1;
            return null;
        });
    }

    public T set(long index, T value) {
        return syncedWrite(() -> {
            if (index >= metaData.size) {
                throw new IndexOutOfBoundsException("Index: " + index + " Size: "+ metaData.size);
            }

            long nextId = metaData.head;
            for (int i = 0; i <= index; i++) {
                current.reset();
                current.setID(nextId);
                chunkService.get().get(current);
                nextId = current.getNext();
            }

            T oldValue = current.getValue();
            current.setValue(value);
            chunkService.put().put(current);

            return null;
        });
    }

    public void applyAll(Consumer<? super T> consumer) {
        syncedWrite(() -> {
            long nextId = metaData.head;
            for (long l = 0; l < metaData.size; l++) {
                current.reset();
                current.setID(nextId);
                chunkService.get().get(current);
                consumer.accept(current.getValue());
                chunkService.put().put(current);
                nextId = current.next;
            }

            return null;
        });
    }

    public T get(final long index) {
        return syncedRead(() -> {
            if (index >= metaData.size) {
                throw new IndexOutOfBoundsException("Index: " + index + " Size: "+ metaData.size);
            }

            long nextId = metaData.head;
            for (int i = 0; i <= index; i++) {
                current.reset();
                current.setID(nextId);
                chunkService.get().get(current);
                nextId = current.next;
            }

            return current.getValue();
        });
    }

    public List<T> getAll() {
        return syncedRead(() -> {
            List<T> list = new ArrayList<>();
            long nextId = metaData.head;
            for (long l = 0; l < metaData.size; l++) {
                current.reset();
                current.setID(nextId);
                chunkService.get().get(current);
                list.add(current.getValue());
                nextId = current.getNext();
            }

            return list;
        });
    }

    public long size() {
        return syncedRead(() -> metaData.size);
    }

    public boolean isEmpty() {
        return syncedRead(() -> metaData.head == ChunkID.INVALID_ID);
    }

    private void ensureSynced() {
        chunkService.get().get(metaData);
    }

    private void updateMetaData() {
        chunkService.put().put(metaData);
    }

    private void acquireReadLock() {
        int chunksLocked = 0;
        while (chunksLocked != 1) {
            chunksLocked = chunkService.lock().lock(true, false, LOCK_TIMEOUT, metaData);
        }
    }

    private void releaseReadLock() {
        int chunksUnlocked = 0;
        while (chunksUnlocked != 1) {
            chunksUnlocked = chunkService.lock().lock(false, false, LOCK_TIMEOUT, metaData);
        }
    }

    private void acquireWriteLock() {
        int chunksLocked = 0;
        while (chunksLocked != 1) {
            chunksLocked = chunkService.lock().lock(true, true, LOCK_TIMEOUT, metaData);
        }
    }

    private void releaseWriteLock() {
        int chunksUnlocked = 0;
        while (chunksUnlocked != 1) {
            chunksUnlocked = chunkService.lock().lock(false, true, LOCK_TIMEOUT, metaData);
        }
    }

    private <S> S syncedRead(Supplier<S> supplier) {
        try {
            acquireReadLock();
            ensureSynced();
            return supplier.get();
        } finally {
            releaseReadLock();
        }
    }

    private <S> S syncedWrite(Supplier<S> supplier) {
        try {
            acquireWriteLock();
            ensureSynced();
            S ret = supplier.get();
            updateMetaData();
            return ret;
        } finally {
            releaseWriteLock();
        }
    }

    /**
     * Retrieves an existing distributed linked list using the specified name.
     */
    public static <T> DistributedLinkedList<T> get(final String name, final ServiceProvider serviceProvider, final Supplier<DistributableValue<T>> valueSupplier) {
        NameserviceService nameService = serviceProvider.getService(NameserviceService.class);
        ChunkService chunkService = serviceProvider.getService(ChunkService.class);

        long chunkId = nameService.getChunkID(name, LOOKUP_TIMEOUT);
        if (chunkId == ChunkID.INVALID_ID) {
            throw new ElementNotFoundException(String.format("Chunk with nameservice id %s not found", name));
        }

        MetaData metaData = new MetaData(chunkId);
        if (!chunkService.get().get(metaData)) {
            throw new ElementNotFoundException(String.format("Chunk wit id %08X not found", chunkId));
        }

        return new DistributedLinkedList<>(metaData, chunkService, valueSupplier);
    }

    /**
     * Creates a new distributed linked list using the specified name if it doesn't exist yet.
     */
    public static <T> DistributedLinkedList<T> create(final String name, final ServiceProvider serviceProvider, final Supplier<DistributableValue<T>> valueSupplier) {
        NameserviceService nameService = serviceProvider.getService(NameserviceService.class);
        ChunkService chunkService = serviceProvider.getService(ChunkService.class);

        if (nameService.getChunkID(name, LOOKUP_TIMEOUT) != ChunkID.INVALID_ID) {
            throw new ElementAlreadyExistsException(String.format("Chunk with nameservice id %s is already registered", name));
        }

        MetaData metaData = new MetaData();
        chunkService.createLocal().create(metaData);
        chunkService.put().put(metaData);
        nameService.register(metaData, name);
        return new DistributedLinkedList<>(metaData, chunkService, valueSupplier);
    }

    /**
     * @return An iterator over a snapshot of this distributed linked list.
     */
    @NotNull
    @Override
    public Iterator<T> iterator() {
        return getAll().iterator();
    }

    private static final class MetaData extends AbstractChunk {

        private long head = ChunkID.INVALID_ID;
        private long tail = ChunkID.INVALID_ID;
        private long size;

        private MetaData() {}

        private MetaData(long chunkID) {
            super(chunkID);
        }

        @Override
        public void exportObject(Exporter exporter) {
            exporter.writeLong(head);
            exporter.writeLong(tail);
            exporter.writeLong(size);
        }

        @Override
        public void importObject(Importer importer) {
            head = importer.readLong(head);
            tail = importer.readLong(tail);
            size = importer.readLong(size);
        }

        @Override
        public int sizeofObject() {
            return 3 * Long.BYTES;
        }
    }

    private static final class DataHolder<T> extends AbstractChunk {

        private final DistributableValue<T> value;
        private long next = ChunkID.INVALID_ID;

        private DataHolder(final DistributableValue<T> value) {
            this.value = value;
        }

        @Override
        public void exportObject(Exporter exporter) {
            exporter.exportObject(value);
            exporter.writeLong(next);
        }

        @Override
        public void importObject(Importer importer) {
            importer.importObject(value);
            next = importer.readLong(next);
        }

        @Override
        public int sizeofObject() {
            return value.sizeofObject() + Long.BYTES;
        }

        public long getNext() {
            return next;
        }

        public void setNext(long next) {
            this.next = next;
        }

        public void setValue(T value) {
            this.value.setValue(value);
        }

        public T getValue() {
            return value.getValue();
        }

        public void reset() {
            value.setValue(null);
            next = ChunkID.INVALID_ID;
            setID(ChunkID.INVALID_ID);
            setState(ChunkState.UNDEFINED);
        }
    }
}
