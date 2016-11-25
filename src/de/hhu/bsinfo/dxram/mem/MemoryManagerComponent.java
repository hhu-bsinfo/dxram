package de.hhu.bsinfo.dxram.mem;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.stats.StatisticsOperation;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorderManager;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.soh.SmallObjectHeap;
import de.hhu.bsinfo.soh.StorageUnsafeMemory;
import de.hhu.bsinfo.utils.unit.StorageUnit;

/**
 * Interface to access the local heap. Features for migration
 * and other tasks are provided as well.
 * Using this class, you have to take care of locking certain calls.
 * This depends on the type (access or manage). Check the documentation
 * of each call to figure how to handle them. Make use of this by combining
 * multiple calls within a single critical section to avoid locking overhead.
 *
 * @author Florian Klein, florian.klein@hhu.de, 13.02.2014
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.11.2015
 */
public final class MemoryManagerComponent extends AbstractDXRAMComponent implements CIDTable.GetNodeIdHook {

    /**
     * Error codes to be returned by some methods.
     */
    public enum MemoryErrorCodes {
        SUCCESS, UNKNOWN, DOES_NOT_EXIST, READ, WRITE, OUT_OF_MEMORY, INVALID_NODE_ROLE,
    }

    // statistics recording
    static final StatisticsOperation SOP_MALLOC = StatisticsRecorderManager.getOperation(MemoryManagerComponent.class, "Malloc");
    private static final Logger LOGGER = LogManager.getFormatterLogger(MemoryManagerComponent.class.getSimpleName());
    private static final StatisticsOperation SOP_FREE = StatisticsRecorderManager.getOperation(MemoryManagerComponent.class, "Free");
    private static final StatisticsOperation SOP_GET = StatisticsRecorderManager.getOperation(MemoryManagerComponent.class, "Get");
    private static final StatisticsOperation SOP_PUT = StatisticsRecorderManager.getOperation(MemoryManagerComponent.class, "Put");
    private static final StatisticsOperation SOP_CREATE = StatisticsRecorderManager.getOperation(MemoryManagerComponent.class, "Create");
    private static final StatisticsOperation SOP_REMOVE = StatisticsRecorderManager.getOperation(MemoryManagerComponent.class, "Remove");
    // configuration values
    @Expose
    private StorageUnit m_keyValueStoreSize = new StorageUnit(128L, StorageUnit.MB);
    // dependent components
    private AbstractBootComponent m_boot;
    private SmallObjectHeap m_rawMemory;
    private CIDTable m_cidTable;
    // private ReentrantReadWriteLock m_lock;
    private AtomicInteger m_lock;
    private long m_numActiveChunks;
    private long m_totalActiveChunkMemory;
    private SmallObjectHeapDataStructureImExporter[] m_imexporter = new SmallObjectHeapDataStructureImExporter[65536];

    /**
     * Constructor
     */
    public MemoryManagerComponent() {
        super(DXRAMComponentOrder.Init.MEMORY, DXRAMComponentOrder.Shutdown.MEMORY);
    }

    /**
     * Get some status information about the memory manager (free, total amount of memory).
     *
     * @return Status information.
     */
    public Status getStatus() {
        Status status = new Status();

        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            status.m_freeMemoryBytes = 0;
            status.m_totalMemoryBytes = 0;
            // #if LOGGER >= ERROR
            LOGGER.error("A %s does not have any memory", role);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        status.m_freeMemoryBytes = m_rawMemory.getStatus().getFree();
        status.m_totalMemoryBytes = m_rawMemory.getStatus().getSize();
        status.m_totalPayloadMemoryBytes = m_rawMemory.getStatus().getAllocatedPayload();
        status.m_numberOfActiveMemoryBlocks = m_rawMemory.getStatus().getAllocatedBlocks();
        status.m_totalChunkPayloadMemory = m_totalActiveChunkMemory;
        status.m_numberOfActiveChunks = m_numActiveChunks;
        status.m_cidTableCount = m_cidTable.getTableCount();
        status.m_totalMemoryCIDTables = m_cidTable.getTotalMemoryTables();

        return status;
    }

    /**
     * Returns the ChunkIDs of all migrated Chunks
     *
     * @return the ChunkIDs of all migrated Chunks
     */
    public ArrayList<Long> getCIDOfAllMigratedChunks() {
        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s does not store any chunks", role);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        return m_cidTable.getCIDOfAllMigratedChunks();
    }

    /**
     * Returns the ChunkID ranges of all locally stored Chunks
     *
     * @return the ChunkID ranges in an ArrayList
     */
    public ArrayList<Long> getCIDRangesOfAllLocalChunks() {
        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s does not store any chunks", role);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        return m_cidTable.getCIDRangesOfAllLocalChunks();
    }

    @Override
    public short getNodeId() {
        return m_boot.getNodeID();
    }

    /**
     * Lock the memory for a management task (create, put, remove).
     */
    public void lockManage() {
        if (m_boot.getNodeRole() == NodeRole.PEER) {
            // m_lock.writeLock().lock();

            // set flag to block further readers from entering
            while (true) {
                int v = m_lock.get();
                if (m_lock.compareAndSet(v, v | 0x40000000)) {
                    break;
                }
            }

            while (!m_lock.compareAndSet(0x40000000, 0x80000000)) {
                // Wait
            }
        }
    }

    /**
     * Lock the memory for an access task (get).
     */
    public void lockAccess() {
        if (m_boot.getNodeRole() == NodeRole.PEER) {

            // m_lock.readLock().lock();

            while (true) {
                int v = m_lock.get() & 0xCFFFFFFF;
                if (m_lock.compareAndSet(v, v + 1)) {
                    break;
                }
            }

        }
    }

    /**
     * Unlock the memory after a management task (create, put, remove).
     */
    public void unlockManage() {
        if (m_boot.getNodeRole() == NodeRole.PEER) {
            // m_lock.writeLock().unlock();

            m_lock.set(0);
        }
    }

    /**
     * Unlock the memory after an access task (get).
     */
    public void unlockAccess() {
        if (m_boot.getNodeRole() == NodeRole.PEER) {
            // m_lock.readLock().unlock();

            m_lock.decrementAndGet();
        }
    }

    // -----------------------------------------------------------------------------

    /**
     * The chunk ID 0 is reserved for a fixed index structure.
     * If the index structure is already created this will delete the old
     * one and allocate a new block of memory with the same id (0).
     *
     * @param p_size
     *     Size for the index chunk.
     * @return On success the chunk id 0, -1 on failure.
     */
    public long createIndex(final int p_size) {
        assert p_size > 0;

        long address;
        long chunkID = -1;

        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s is not allowed to create an index chunk", role);
            // #endif /* LOGGER >= ERROR */
            return chunkID;
        }

        if (p_size > SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK) {
            // #if LOGGER >= WARN
            LOGGER.warn("Performance warning, creating a chunk with size %d exceeding max size %d", p_size, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK);
            // #endif /* LOGGER >= WARN */
        }

        if (m_cidTable.get(0) != 0) {
            // delete old entry
            address = m_cidTable.delete(0, false);
            m_rawMemory.free(address);
            m_totalActiveChunkMemory -= m_rawMemory.getSizeBlock(address);
            m_numActiveChunks--;
        }

        address = m_rawMemory.malloc(p_size);
        if (address >= 0) {
            chunkID = (long) m_boot.getNodeID() << 48;
            // register new chunk in cid table
            if (!m_cidTable.set(chunkID, address)) {
                // on demand allocation of new table failed
                // free previously created chunk for data to avoid memory leak
                m_rawMemory.free(address);
                chunkID = -1;
            } else {
                m_numActiveChunks++;
                m_totalActiveChunkMemory += p_size;
            }
        } else {
            chunkID = -1;
        }

        return chunkID;
    }

    /**
     * Create a chunk with a specific chunk id (used for migration/recovery).
     *
     * @param p_chunkId
     *     Chunk id to assign to the chunk.
     * @param p_size
     *     Size of the chunk.
     * @return The chunk id if successful, -1 if another chunk with the same id already exists or allocation memory failed.
     */
    public long create(final long p_chunkId, final int p_size) {
        assert p_size > 0;

        long address;
        long chunkID = -1;

        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s is not allowed to create a chunk with an id", role);
            // #endif /* LOGGER >= ERROR */
            return chunkID;
        }

        chunkID = p_chunkId;

        if (p_size > SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK) {
            // #if LOGGER >= WARN
            LOGGER.warn("Performance warning, creating a chunk with size %d exceeding max size %d", p_size, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK);
            // #endif /* LOGGER >= WARN */
        }

        // verify this id is not used
        if (m_cidTable.get(p_chunkId) == 0) {
            address = m_rawMemory.malloc(p_size);
            if (address >= 0) {
                // register new chunk
                // register new chunk in cid table
                if (!m_cidTable.set(chunkID, address)) {
                    // on demand allocation of new table failed
                    // free previously created chunk for data to avoid memory leak
                    m_rawMemory.free(address);
                    chunkID = -1;
                } else {
                    m_numActiveChunks++;
                    m_totalActiveChunkMemory += p_size;
                    chunkID = p_chunkId;
                }
            } else {
                chunkID = -1;
            }
        }

        return chunkID;
    }

    public long[] createMultiSizes(final int... p_sizes) {
        long[] addresses;
        long[] lids;

        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s is not allowed to create a chunk", role);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        // #ifdef STATISTICS
        SOP_CREATE.enter();
        // #endif /* STATISTICS */

        // TODO lid multi create

        // get new LIDs
        lids = new long[p_sizes.length];
        for (int i = 0; i < lids.length; i++) {
            lids[i] = m_cidTable.getFreeLID();
            if (lids[i] == -1) {
                // TODO error handling
                return null;
            }
        }

        // first, try to allocate. maybe early return
        // #ifdef STATISTICS
        // TODO multi malloc
        //  SOP_MALLOC.enter(p_size);
        // #endif /* STATISTICS */
        addresses = m_rawMemory.multiMallocSizes(p_sizes);
        // #ifdef STATISTICS
        //SOP_MALLOC.leave();
        // #endif /* STATISTICS */
        if (addresses != null) {

            for (int i = 0; i < lids.length; i++) {
                lids[i] = ((long) m_boot.getNodeID() << 48) + lids[i];

                // TODO multi set?

                // register new chunk in cid table
                if (!m_cidTable.set(lids[i], addresses[i])) {
                    // on demand allocation of new table failed
                    // free previously created chunk for data to avoid memory leak
                    m_rawMemory.free(addresses[i]);

                    // TODO error handling
                } else {
                    m_numActiveChunks++;
                    m_totalActiveChunkMemory += p_sizes[i];
                }
            }

        } else {
            // most likely out of memory
            // #if LOGGER >= ERROR
            //            LOGGER.fatal("Creating chunk with size %d failed, most likely out of memory, free %d, total %d", p_size, m_rawMemory.getStatus().getFree(),
            //                m_rawMemory.getStatus().getSize());
            // #endif /* LOGGER >= ERROR */

            // put lids back
            // TODO multi put back
            for (int i = 0; i < lids.length; i++) {
                m_cidTable.putChunkIDForReuse(lids[i]);
            }
        }

        // #ifdef STATISTICS
        //SOP_CREATE.leave();
        // #endif /* STATISTICS */

        return lids;
    }

    public long[] createMulti(final DataStructure... p_dataStructures) {
        int[] sizes = new int[p_dataStructures.length];

        for (int i = 0; i < p_dataStructures.length; i++) {
            sizes[i] = p_dataStructures[i].sizeofObject();
        }

        return createMultiSizes(sizes);
    }

    public long[] createMulti(final int p_size, final int p_count) {
        long[] addresses;
        long[] lids;

        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s is not allowed to create a chunk", role);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        // #ifdef STATISTICS
        SOP_CREATE.enter();
        // #endif /* STATISTICS */

        // TODO lid multi create

        // get new LIDs
        lids = new long[p_count];
        for (int i = 0; i < lids.length; i++) {
            lids[i] = m_cidTable.getFreeLID();
            if (lids[i] == -1) {
                // TODO error handling
                return null;
            }
        }

        // first, try to allocate. maybe early return
        // #ifdef STATISTICS
        // TODO multi malloc
        //  SOP_MALLOC.enter(p_size);
        // #endif /* STATISTICS */
        addresses = m_rawMemory.multiMalloc(p_size, p_count);
        // #ifdef STATISTICS
        //SOP_MALLOC.leave();
        // #endif /* STATISTICS */
        if (addresses != null) {

            for (int i = 0; i < lids.length; i++) {
                lids[i] = ((long) m_boot.getNodeID() << 48) + lids[i];

                // TODO multi set?

                // register new chunk in cid table
                if (!m_cidTable.set(lids[i], addresses[i])) {
                    // on demand allocation of new table failed
                    // free previously created chunk for data to avoid memory leak
                    m_rawMemory.free(addresses[i]);

                    // TODO error handling
                } else {
                    m_numActiveChunks++;
                    m_totalActiveChunkMemory += p_size;
                }
            }

        } else {
            // most likely out of memory
            // #if LOGGER >= ERROR
            //            LOGGER.fatal("Creating chunk with size %d failed, most likely out of memory, free %d, total %d", p_size, m_rawMemory.getStatus().getFree(),
            //                m_rawMemory.getStatus().getSize());
            // #endif /* LOGGER >= ERROR */

            // put lids back
            // TODO multi put back
            for (int i = 0; i < lids.length; i++) {
                m_cidTable.putChunkIDForReuse(lids[i]);
            }
        }

        // #ifdef STATISTICS
        //SOP_CREATE.leave();
        // #endif /* STATISTICS */

        return lids;
    }

    /**
     * Create a new chunk.
     * This is a management call and has to be locked using lockManage().
     *
     * @param p_size
     *     Size in bytes of the payload the chunk contains.
     * @return Address of the allocated chunk or -1 if creating the chunk failed.
     */
    public long create(final int p_size) {
        assert p_size > 0;

        long address;
        long chunkID = -1;
        long lid;

        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s is not allowed to create a chunk", role);
            // #endif /* LOGGER >= ERROR */
            return chunkID;
        }

        if (p_size > SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK) {
            // #if LOGGER >= WARN
            LOGGER.warn("Performance warning, creating a chunk with size %d exceeding max size %d", p_size, SmallObjectHeap.MAX_SIZE_MEMORY_BLOCK);
            // #endif /* LOGGER >= WARN */
        }

        // #ifdef STATISTICS
        SOP_CREATE.enter();
        // #endif /* STATISTICS */

        // get new LID from CIDTable
        lid = m_cidTable.getFreeLID();
        if (lid == -1) {
            chunkID = -1;
        } else {
            // first, try to allocate. maybe early return
            // #ifdef STATISTICS
            SOP_MALLOC.enter(p_size);
            // #endif /* STATISTICS */
            address = m_rawMemory.malloc(p_size);
            // #ifdef STATISTICS
            SOP_MALLOC.leave();
            // #endif /* STATISTICS */
            if (address >= 0) {
                chunkID = ((long) m_boot.getNodeID() << 48) + lid;
                // register new chunk in cid table
                if (!m_cidTable.set(chunkID, address)) {
                    // on demand allocation of new table failed
                    // free previously created chunk for data to avoid memory leak
                    m_rawMemory.free(address);
                    chunkID = -1;
                } else {
                    m_numActiveChunks++;
                    m_totalActiveChunkMemory += p_size;
                }
            } else {
                // most likely out of memory
                // #if LOGGER >= ERROR
                LOGGER.fatal("Creating chunk with size %d failed, most likely out of memory, free %d, total %d", p_size, m_rawMemory.getStatus().getFree(),
                    m_rawMemory.getStatus().getSize());
                // #endif /* LOGGER >= ERROR */

                // put lid back
                m_cidTable.putChunkIDForReuse(lid);
            }
        }

        // #ifdef STATISTICS
        SOP_CREATE.leave();
        // #endif /* STATISTICS */

        return chunkID;
    }

    /**
     * Get the size of a chunk (payload only, i.e. minus size for version).
     * This is an access call and has to be locked using lockAccess().
     *
     * @param p_chunkID
     *     ChunkID of the chunk, the local id gets extracted, the node ID ignored.
     * @return Size of the chunk or -1 if the chunkID was invalid.
     */
    public int getSize(final long p_chunkID) {
        long address;
        int size = -1;

        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s does not store any chunks", role);
            // #endif /* LOGGER >= ERROR */
            return size;
        }

        address = m_cidTable.get(p_chunkID);
        if (address > 0) {
            size = m_rawMemory.getSizeBlock(address);
        }

        return size;
    }

    /**
     * Get the payload of a chunk.
     * This is an access call and has to be locked using lockAccess().
     *
     * @param p_dataStructure
     *     Data structure to write the data of its specified ID to.
     * @return True if getting the chunk payload was successful, false if no chunk with the ID specified by the data structure exists.
     */
    public MemoryErrorCodes get(final DataStructure p_dataStructure) {
        long address;
        MemoryErrorCodes ret = MemoryErrorCodes.SUCCESS;

        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            ret = MemoryErrorCodes.DOES_NOT_EXIST;
            return ret;
        }

        // #ifdef STATISTICS
        SOP_GET.enter();
        // #endif /* STATISTICS */

        address = m_cidTable.get(p_dataStructure.getID());
        if (address > 0) {
            // pool the im/exporters
            SmallObjectHeapDataStructureImExporter importer = getImExporter(address);

            // SmallObjectHeapDataStructureImExporter importer =
            // new SmallObjectHeapDataStructureImExporter(m_rawMemory, address, 0, chunkSize);
            importer.importObject(p_dataStructure);
        } else {
            ret = MemoryErrorCodes.DOES_NOT_EXIST;
        }

        // #ifdef STATISTICS
        SOP_GET.leave();
        // #endif /* STATISTICS */

        return ret;
    }

    /**
     * Get a chunk when size is unknown.
     * This is an access call and has to be locked using lockAccess().
     *
     * @param p_chunkID
     *     Data structure to write the data of its specified ID to.
     * @return A byte array with payload if getting the chunk payload was successful, null if no chunk with the ID exists.
     */
    public byte[] get(final long p_chunkID) {
        byte[] ret = null;
        long address;

        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            return null;
        }

        // #ifdef STATISTICS
        SOP_GET.enter();
        // #endif /* STATISTICS */

        address = m_cidTable.get(p_chunkID);
        if (address > 0) {
            int chunkSize = m_rawMemory.getSizeBlock(address);
            ret = new byte[chunkSize];

            // pool the im/exporters
            SmallObjectHeapDataStructureImExporter importer = getImExporter(address);
            // SmallObjectHeapDataStructureImExporter importer =
            // new SmallObjectHeapDataStructureImExporter(m_rawMemory, address, 0, chunkSize);
            if (importer.readBytes(ret) != chunkSize) {
                ret = null;
            }
        } else {
            // #if LOGGER >= WARN
            LOGGER.warn("Could not find data for ID=0x%X", p_chunkID);
            // #endif /* LOGGER >= WARN */
        }

        // #ifdef STATISTICS
        SOP_GET.leave();
        // #endif /* STATISTICS */

        return ret;
    }

    /**
     * Put some data into a chunk.
     * This is an access call and has to be locked using lockAccess().
     * Note: lockAccess() does NOT take care of data races of the data to write.
     * The caller has to take care of proper locking to avoid consistency issue with his data.
     *
     * @param p_dataStructure
     *     Data structure to put
     * @return MemoryErrorCodes indicating success or failure
     */
    public MemoryErrorCodes put(final DataStructure p_dataStructure) {
        long address;
        MemoryErrorCodes ret = MemoryErrorCodes.SUCCESS;

        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            ret = MemoryErrorCodes.DOES_NOT_EXIST;
            return ret;
        }

        // #ifdef STATISTICS
        SOP_PUT.enter();
        // #endif /* STATISTICS */

        address = m_cidTable.get(p_dataStructure.getID());
        if (address > 0) {
            // pool the im/exporters
            SmallObjectHeapDataStructureImExporter exporter = getImExporter(address);
            // SmallObjectHeapDataStructureImExporter exporter =
            // new SmallObjectHeapDataStructureImExporter(m_rawMemory, address, 0, chunkSize);
            exporter.exportObject(p_dataStructure);
        } else {
            ret = MemoryErrorCodes.DOES_NOT_EXIST;
        }

        // #ifdef STATISTICS
        SOP_PUT.leave();
        // #endif /* STATISTICS */

        return ret;
    }

    /**
     * Removes a Chunk from the memory
     * This is a management call and has to be locked using lockManage().
     *
     * @param p_chunkID
     *     the ChunkID of the Chunk
     * @param p_wasMigrated
     *     default value for this parameter should be false!
     *     if chunk was deleted during migration this flag should be set to true
     * @return MemoryErrorCodes indicating success or failure
     */
    public MemoryErrorCodes remove(final long p_chunkID, final boolean p_wasMigrated) {
        long addressDeletedChunk;
        int size;
        MemoryErrorCodes ret = MemoryErrorCodes.SUCCESS;

        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s does not store any chunks", role);
            // #endif /* LOGGER >= ERROR */
            ret = MemoryErrorCodes.INVALID_NODE_ROLE;
            return ret;
        }

        // #ifdef STATISTICS
        SOP_REMOVE.enter();
        // #endif /* STATISTICS */

        // Get and delete the address from the CIDTable, mark as zombie first
        addressDeletedChunk = m_cidTable.delete(p_chunkID, true);
        if (addressDeletedChunk != -1) {
            // more space for another zombie for reuse in LID store?
            if (p_wasMigrated) {

                m_cidTable.delete(p_chunkID, false);
            } else {

                if (m_cidTable.putChunkIDForReuse(ChunkID.getLocalID(p_chunkID))) {
                    // detach reference to zombie
                    m_cidTable.delete(p_chunkID, false);
                } else {
                    // no space for zombie in LID store, keep him "alive" in table
                }
            }
            size = m_rawMemory.getSizeBlock(addressDeletedChunk);
            // #ifdef STATISTICS
            SOP_FREE.enter(size);
            // #endif /* STATISTICS */
            m_rawMemory.free(addressDeletedChunk);
            // #ifdef STATISTICS
            SOP_FREE.leave();
            // #endif /* STATISTICS */
            m_numActiveChunks--;
            m_totalActiveChunkMemory -= size;
        } else {
            ret = MemoryErrorCodes.DOES_NOT_EXIST;
        }

        // #ifdef STATISTICS
        SOP_REMOVE.leave();
        // #endif /* STATISTICS */

        return ret;
    }

    public MemoryErrorCodes createAndPutRecovered(final long[] p_chunkIDs, final byte[] p_data, final int[] p_offsets, final int[] p_lengths,
        final int p_usedEntries) {

        // TODO: needs create with chunk ID call + put in tight loop

    }

    // -----------------------------------------------------------------------------

    /**
     * Read a single byte from a chunk. Use this if you need to access a very specific value
     * once to avoid reading a huge chunk. Prefer the get-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *     Chunk id of the chunk to read.
     * @param p_offset
     *     Offset within the chunk to read.
     * @return The value read at the offset of the chunk.
     */
    public byte readByte(final long p_chunkID, final int p_offset) {
        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s does not store any chunks", role);
            // #endif /* LOGGER >= ERROR */
            return -1;
        }

        long address = m_cidTable.get(p_chunkID);
        if (address > 0) {
            return m_rawMemory.readByte(address, p_offset);
        } else {
            return -1;
        }
    }

    /**
     * Read a single short from a chunk. Use this if you need to access a very specific value
     * once to avoid reading a huge chunk. Prefer the get-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *     Chunk id of the chunk to read.
     * @param p_offset
     *     Offset within the chunk to read.
     * @return The value read at the offset of the chunk.
     */
    public short readShort(final long p_chunkID, final int p_offset) {
        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s does not store any chunks", role);
            // #endif /* LOGGER >= ERROR */
            return -1;
        }

        long address = m_cidTable.get(p_chunkID);
        if (address > 0) {
            return m_rawMemory.readShort(address, p_offset);
        } else {
            return -1;
        }
    }

    /**
     * Read a single int from a chunk. Use this if you need to access a very specific value
     * once to avoid reading a huge chunk. Prefer the get-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *     Chunk id of the chunk to read.
     * @param p_offset
     *     Offset within the chunk to read.
     * @return The value read at the offset of the chunk.
     */
    public int readInt(final long p_chunkID, final int p_offset) {
        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s does not store any chunks", role);
            // #endif /* LOGGER >= ERROR */
            return -1;
        }

        long address = m_cidTable.get(p_chunkID);
        if (address > 0) {
            return m_rawMemory.readInt(address, p_offset);
        } else {
            return -1;
        }
    }

    /**
     * Read a single long from a chunk. Use this if you need to access a very specific value
     * once to avoid reading a huge chunk. Prefer the get-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *     Chunk id of the chunk to read.
     * @param p_offset
     *     Offset within the chunk to read.
     * @return The value read at the offset of the chunk.
     */
    public long readLong(final long p_chunkID, final int p_offset) {
        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s does not store any chunks", role);
            // #endif /* LOGGER >= ERROR */
            return -1;
        }

        long address = m_cidTable.get(p_chunkID);
        if (address > 0) {
            return m_rawMemory.readLong(address, p_offset);
        } else {
            return -1;
        }
    }

    /**
     * Write a single byte to a chunk. Use this if you need to access a very specific value
     * once to avoid writing a huge chunk. Prefer the put-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *     Chunk id of the chunk to write.
     * @param p_offset
     *     Offset within the chunk to write.
     * @param p_value
     *     Value to write.
     * @return True if writing chunk was successful, false otherwise.
     */
    public boolean writeByte(final long p_chunkID, final int p_offset, final byte p_value) {
        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s does not store any chunks", role);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        long address = m_cidTable.get(p_chunkID);
        if (address > 0) {
            m_rawMemory.writeByte(address, p_offset, p_value);
        } else {
            return false;
        }

        return true;
    }

    /**
     * Write a single short to a chunk. Use this if you need to access a very specific value
     * once to avoid writing a huge chunk. Prefer the put-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *     Chunk id of the chunk to write.
     * @param p_offset
     *     Offset within the chunk to write.
     * @param p_value
     *     Value to write.
     * @return True if writing chunk was successful, false otherwise.
     */
    public boolean writeShort(final long p_chunkID, final int p_offset, final short p_value) {
        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s does not store any chunks", role);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        long address = m_cidTable.get(p_chunkID);
        if (address > 0) {
            m_rawMemory.writeShort(address, p_offset, p_value);
        } else {
            return false;
        }

        return true;
    }

    /**
     * Write a single int to a chunk. Use this if you need to access a very specific value
     * once to avoid writing a huge chunk. Prefer the put-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *     Chunk id of the chunk to write.
     * @param p_offset
     *     Offset within the chunk to write.
     * @param p_value
     *     Value to write.
     * @return True if writing chunk was successful, false otherwise.
     */
    public boolean writeInt(final long p_chunkID, final int p_offset, final int p_value) {
        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s does not store any chunks", role);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        long address = m_cidTable.get(p_chunkID);
        if (address > 0) {
            m_rawMemory.writeInt(address, p_offset, p_value);
        } else {
            return false;
        }

        return true;
    }

    /**
     * Write a single long to a chunk. Use this if you need to access a very specific value
     * once to avoid writing a huge chunk. Prefer the put-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *     Chunk id of the chunk to write.
     * @param p_offset
     *     Offset within the chunk to write.
     * @param p_value
     *     Value to write.
     * @return True if writing chunk was successful, false otherwise.
     */
    public boolean writeLong(final long p_chunkID, final int p_offset, final long p_value) {
        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s does not store any chunks", role);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        long address = m_cidTable.get(p_chunkID);
        if (address > 0) {
            m_rawMemory.writeLong(address, p_offset, p_value);
        } else {
            return false;
        }

        return true;
    }

    // -----------------------------------------------------------------------------

    /**
     * Returns whether this Chunk is stored locally or not.
     * Only the LID is evaluated and checked. The NID is masked out.
     * This is an access call and has to be locked using lockAccess().
     *
     * @param p_chunkID
     *     the ChunkID
     * @return whether this Chunk is stored locally or not
     */
    public boolean exists(final long p_chunkID) {
        long address;

        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            return false;
        }

        // Get the address from the CIDTable
        address = m_cidTable.get(p_chunkID);

        // If address <= 0, the Chunk does not exists in memory
        return address > 0;
    }

    /**
     * Returns whether this Chunk was migrated here or not
     *
     * @param p_chunkID
     *     the ChunkID
     * @return whether this Chunk was migrated here or not
     */
    public boolean dataWasMigrated(final long p_chunkID) {
        return ChunkID.getCreatorID(p_chunkID) != m_boot.getNodeID();
    }

    /**
     * Removes the ChunkID of a deleted Chunk that was migrated
     *
     * @param p_chunkID
     *     the ChunkID
     */
    public void prepareChunkIDForReuse(final long p_chunkID) {
        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s does not store any chunks", role);
            // #endif /* LOGGER >= ERROR */
            return;
        }

        m_cidTable.putChunkIDForReuse(p_chunkID);
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        if (p_engineEngineSettings.getRole() == NodeRole.PEER) {
            // #if LOGGER == INFO
            LOGGER.info("Allocating native memory (%d mb). This may take a while...", m_keyValueStoreSize.getMB());
            // #endif /* LOGGER == INFO */
            m_rawMemory = new SmallObjectHeap(new StorageUnsafeMemory(), m_keyValueStoreSize.getBytes());
            m_cidTable = new CIDTable(this);
            m_cidTable.initialize(m_rawMemory);

            // m_lock = new ReentrantReadWriteLock(false);
            m_lock = new AtomicInteger(0);

            m_numActiveChunks = 0;
            m_totalActiveChunkMemory = 0;
        }

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        if (m_boot.getNodeRole() == NodeRole.PEER) {
            m_cidTable.disengage();
            m_rawMemory.destroy();

            m_cidTable = null;
            m_rawMemory = null;
            m_lock = null;
        }

        return true;
    }

    /**
     * Pooling the im/exporters to lower memory footprint.
     *
     * @param p_address
     *     Start address of the chunk
     * @return Im/Exporter for the chunk
     */
    private SmallObjectHeapDataStructureImExporter getImExporter(final long p_address) {
        long tid = Thread.currentThread().getId();
        if (tid > 65536) {
            throw new RuntimeException("Exceeded max. thread id");
        }

        // pool the im/exporters
        SmallObjectHeapDataStructureImExporter importer = m_imexporter[(int) tid];
        if (importer == null) {
            m_imexporter[(int) tid] = new SmallObjectHeapDataStructureImExporter(m_rawMemory, p_address, 0);
            importer = m_imexporter[(int) tid];
        } else {
            importer.setAllocatedMemoryStartAddress(p_address);
            importer.setOffset(0);
        }

        return importer;
    }

    /**
     * Object containing status information about the memory.
     *
     * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
     */
    public static class Status {
        private long m_totalMemoryBytes = -1;
        private long m_freeMemoryBytes = -1;
        private long m_totalPayloadMemoryBytes = -1;
        private long m_numberOfActiveMemoryBlocks = -1;
        private long m_numberOfActiveChunks = -1;
        private long m_totalChunkPayloadMemory = -1;
        private long m_cidTableCount = -1;
        private long m_totalMemoryCIDTables = -1;

        /**
         * Constructor
         */
        public Status() {

        }

        /**
         * Get the total amount of memory in bytes.
         *
         * @return Total amount of memory in bytes.
         */
        public long getTotalMemory() {
            return m_totalMemoryBytes;
        }

        /**
         * Get the total amount of free memory in bytes.
         *
         * @return Amount of free memory in bytes.
         */
        public long getFreeMemory() {
            return m_freeMemoryBytes;
        }

        /**
         * Get the total amount of allocated memory used for payload/chunk data.
         *
         * @return Amount of memory allocated for payload in bytes.
         */
        public long getTotalPayloadMemory() {
            return m_totalPayloadMemoryBytes;
        }

        /**
         * Get the number of active/allocated memory blocks.
         *
         * @return Number of active blocks.
         */
        public long getNumberOfActiveMemoryBlocks() {
            return m_numberOfActiveMemoryBlocks;
        }

        /**
         * Get the number of currently allocated chunks.
         *
         * @return Number of currently allocated chunks.
         */
        public long getNumberOfActiveChunks() {
            return m_numberOfActiveChunks;
        }

        /**
         * Get the total amount of memory used for chunk payload.
         *
         * @return Total amount of memory in bytes usable for chunk payload.
         */
        public long getTotalChunkMemory() {
            return m_totalChunkPayloadMemory;
        }

        /**
         * Get the number of cid tables.
         *
         * @return Number of cid tables.
         */
        public long getCIDTableCount() {
            return m_cidTableCount;
        }

        /**
         * Get the amount of memory used by the cid tables.
         *
         * @return Memory used by cid tables (in bytes).
         */
        public long getTotalMemoryCIDTables() {
            return m_totalMemoryCIDTables;
        }
    }
}
