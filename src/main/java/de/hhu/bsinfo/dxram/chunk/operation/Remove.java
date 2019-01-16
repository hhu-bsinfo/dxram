package de.hhu.bsinfo.dxram.chunk.operation;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.chunk.messages.RemoveMessage;
import de.hhu.bsinfo.dxram.chunk.messages.ReuseIDMessage;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.lookup.LookupState;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.ArrayListLong;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.TimePool;
import de.hhu.bsinfo.dxutils.stats.ValuePool;

/**
 * Remove a chunk from the key-value memory (locally and remotely)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class Remove extends AbstractOperation implements MessageReceiver {
    private static final TimePool SOP_REMOVE_TIME = new TimePool(ChunkService.class, "Remove");
    private static final TimePool SOP_INCOMING_REMOVE_TIME = new TimePool(ChunkService.class, "RemoveIncoming");
    private static final ValuePool SOP_REMOVE = new ValuePool(ChunkService.class, "Remove");
    private static final ValuePool SOP_INCOMING_REMOVE = new ValuePool(ChunkService.class, "RemoveIncoming");

    static {
        StatisticsManager.get().registerOperation(Remove.class, SOP_REMOVE_TIME);
        StatisticsManager.get().registerOperation(Remove.class, SOP_INCOMING_REMOVE_TIME);
        StatisticsManager.get().registerOperation(Remove.class, SOP_REMOVE);
        StatisticsManager.get().registerOperation(Remove.class, SOP_INCOMING_REMOVE_TIME);
    }

    private ChunkRemover m_remover;

    /**
     * Constructor
     *
     * @param p_parentService
     *         Instance of parent service this operation belongs to
     * @param p_boot
     *         Instance of BootComponent
     * @param p_backup
     *         Instance of BackupComponent
     * @param p_chunk
     *         Instance of ChunkComponent
     * @param p_network
     *         Instance of NetworkComponent
     * @param p_lookup
     *         Instance of LookupComponent
     * @param p_nameservice
     *         Instance of NameserviceComponent
     */
    public Remove(final Class<? extends AbstractDXRAMService> p_parentService,
            final AbstractBootComponent p_boot, final BackupComponent p_backup, final ChunkComponent p_chunk,
            final NetworkComponent p_network, final LookupComponent p_lookup,
            final NameserviceComponent p_nameservice, final int p_removerQueueSize) {
        super(p_parentService, p_boot, p_backup, p_chunk, p_network, p_lookup, p_nameservice);

        m_remover = new ChunkRemover(p_removerQueueSize);
        m_remover.start();

        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_REMOVE_MESSAGE,
                RemoveMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_REUSE_ID_MESSAGE,
                ReuseIDMessage.class);

        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_REMOVE_MESSAGE, this);
        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_REUSE_ID_MESSAGE, this);
    }

    /**
     * Remove one or multiple chunks
     *
     * @param p_chunks
     *         Array chunks to remove
     * @return Number of elements removed. If less, than expected one or multiple objects could not be removed
     */
    public int remove(final AbstractChunk... p_chunks) {
        return remove(p_chunks, 0, p_chunks.length);
    }

    /**
     * Remove one or multiple chunks
     *
     * @param p_cids
     *         Array of CIDs of chunks to remove
     * @return Number of elements removed. If less, than expected one or multiple objects could not be removed
     */
    public int remove(final long... p_cids) {
        return remove(p_cids, 0, p_cids.length);
    }

    /**
     * Remove one or multiple chunks
     *
     * @param p_chunks
     *         Array chunks to remove
     * @param p_offset
     *         Offset in array where to start remove operations
     * @param p_count
     *         Number of chunks to remove (might be less array size/number of chunks provided)
     * @return Number of elements removed. If less, than expected one or multiple objects could not be removed
     */
    public int remove(final AbstractChunk[] p_chunks, final int p_offset, final int p_count) {

        // TODO set data structure chunk states

        long[] chunkIDs = new long[p_chunks.length];

        for (int i = 0; i < chunkIDs.length; i++) {
            chunkIDs[i] = p_chunks[i].getID();
        }

        return remove(chunkIDs);
    }

    /**
     * Remove one or multiple chunks
     *
     * @param p_cids
     *         Array of CIDs of chunks to remove
     * @param p_offset
     *         Offset in array where to start remove operations
     * @param p_count
     *         Number of chunks to remove (might be less array size/number of chunks provided)
     * @return Number of elements removed. If less, than expected one or multiple objects could not be removed
     */
    public int remove(final long[] p_cids, final int p_offset, final int p_count) {
        int chunksRemoved = 0;
        int size;

        m_logger.trace("remove[cids.length %d, offset %d, count %d: %s]", p_cids.length, p_offset, p_count,
                Arrays.toString(p_cids));

        SOP_REMOVE.add(p_count);
        SOP_REMOVE_TIME.start();

        // sort by local and remote data first
        Map<Short, ArrayListLong> remoteChunksByPeers = new TreeMap<>();
        Map<Long, ArrayListLong> remoteChunksByBackupPeers = new TreeMap<>();
        ArrayListLong localChunks = new ArrayListLong();
        Map<Short, ArrayListLong> reuseChunkIDsByPeers = new TreeMap<>();

        for (int i = 0; i < p_count; i++) {
            // invalid values allowed -> filter
            if (p_cids[i + p_offset] == ChunkID.INVALID_ID) {
                continue;
            }

            if (m_chunk.isStorageEnabled() && m_chunk.getMemory().exists().exists(p_cids[i + p_offset])) {
                if (ChunkID.getCreatorID(p_cids[i + p_offset]) != m_boot.getNodeId()) {
                    // sort by initial owner/creator for chunk ID reuse
                    ArrayListLong reuseChunkIDsOfPeer = reuseChunkIDsByPeers.computeIfAbsent(ChunkID.getCreatorID(
                            p_cids[i + p_offset]), a -> new ArrayListLong());
                    reuseChunkIDsOfPeer.add(p_cids[i + p_offset]);
                }

                // local and locally stored migrated chunks
                localChunks.add(p_cids[i + p_offset]);

                if (m_backup.isActive()) {
                    // sort by backup peers
                    long backupPeersAsLong = m_backup.getBackupPeersForLocalChunks(p_cids[i + p_offset]);
                    ArrayListLong remoteChunkIDsOfBackupPeers = remoteChunksByBackupPeers.computeIfAbsent(
                            backupPeersAsLong, a -> new ArrayListLong());
                    remoteChunkIDsOfBackupPeers.add(p_cids[i + p_offset]);
                }
            } else {
                // remote or migrated, figure out location and sort by peers
                LookupRange location = m_lookup.getLookupRange(p_cids[i + p_offset]);

                while (location.getState() == LookupState.DATA_TEMPORARY_UNAVAILABLE) {
                    try {
                        Thread.sleep(100);
                    } catch (final InterruptedException ignore) {
                    }

                    location = m_lookup.getLookupRange(p_cids[i + p_offset]);
                }

                if (location.getState() == LookupState.OK) {
                    short peer = location.getPrimaryPeer();

                    ArrayListLong remoteChunksOfPeer = remoteChunksByPeers.computeIfAbsent(peer,
                            a -> new ArrayListLong());
                    remoteChunksOfPeer.add(p_cids[i + p_offset]);
                }
            }
        }

        // remove local chunks from superpeer overlay first, so cannot be found before being deleted
        m_lookup.removeChunkIDs(localChunks);

        // remove local chunkIDs
        for (int i = 0; i < localChunks.getSize(); i++) {
            size = m_chunk.getMemory().remove().remove(localChunks.get(i));

            if (size > 0) {
                chunksRemoved++;
                m_backup.deregisterChunk(localChunks.get(i), size);
            } else {
                m_logger.error("Removing chunk ID 0x%X failed: %s", localChunks.get(i), ChunkState.values()[-size]);
            }
        }

        // send message to initial creator of locally stored but migrated removed chunks to allow re-use of chunk ID,
        // otherwise chunk ID gets lost here
        for (final Map.Entry<Short, ArrayListLong> reuseChunkIDs : reuseChunkIDsByPeers.entrySet()) {
            short peer = reuseChunkIDs.getKey();
            ArrayListLong chunkIDs = reuseChunkIDs.getValue();

            ReuseIDMessage message = new ReuseIDMessage(peer, chunkIDs);

            try {
                m_network.sendMessage(message);
            } catch (final NetworkException e) {
                m_logger.error("Sending reuse chunk ID message to peer 0x%X failed: %s", peer, e);
            }
        }

        // go for remote ones by each peer
        for (final Map.Entry<Short, ArrayListLong> peerWithChunks : remoteChunksByPeers.entrySet()) {
            short peer = peerWithChunks.getKey();
            ArrayListLong remoteChunks = peerWithChunks.getValue();

            if (peer == m_boot.getNodeId()) {
                // local remove, migrated data to current node
                // remove migrated chunks from superpeer overlay first, so cannot be found before being deleted
                m_lookup.removeChunkIDs(remoteChunks);

                for (int i = 0; i < remoteChunks.getSize(); i++) {
                    size = m_chunk.getMemory().remove().remove(localChunks.get(i), false);

                    if (size > 0) {
                        chunksRemoved++;
                        m_backup.deregisterChunk(localChunks.get(i), size);
                    } else {
                        m_logger.error("Removing chunk ID 0x%X failed: %s", localChunks.get(i),
                                ChunkState.values()[-size]);
                    }
                }
            } else {
                // Remote remove from specified peer
                RemoveMessage message = new RemoveMessage(peer, remoteChunks);

                try {
                    m_network.sendMessage(message);
                } catch (final NetworkException e) {
                    m_logger.error("Sending chunk remove to peer 0x%X failed: %s", peer, e);
                    continue;
                }

                chunksRemoved += remoteChunks.getSize();
            }
        }

        // Inform backups
        if (m_backup.isActive()) {
            long backupPeersAsLong;
            BackupPeer[] backupPeers;
            ArrayListLong ids;

            for (Map.Entry<Long, ArrayListLong> entry : remoteChunksByBackupPeers.entrySet()) {
                backupPeersAsLong = entry.getKey();
                ids = entry.getValue();

                backupPeers = BackupRange.convert(backupPeersAsLong);

                for (BackupPeer backupPeer : backupPeers) {
                    if (backupPeer != null && backupPeer.getNodeID() != m_boot.getNodeId()) {

                        try {
                            m_network.sendMessage(new de.hhu.bsinfo.dxram.log.messages.RemoveMessage(
                                    backupPeer.getNodeID(), ids));
                        } catch (final NetworkException ignore) {

                        }
                    }
                }
            }
        }

        SOP_REMOVE_TIME.stop();

        return chunksRemoved;
    }

    @Override
    public void onIncomingMessage(final Message p_message) {
        if (p_message.getType() == DXRAMMessageTypes.CHUNK_MESSAGES_TYPE) {
            switch (p_message.getSubtype()) {
                case ChunkMessages.SUBTYPE_REMOVE_MESSAGE:
                    incomingRemoveMessage((RemoveMessage) p_message);
                    break;
                case ChunkMessages.SUBTYPE_REUSE_ID_MESSAGE:
                    incomingReuseIDMessage((ReuseIDMessage) p_message);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * Handles an incoming RemoveMessage
     *
     * @param p_message
     *         the RemoveMessage
     */
    private void incomingRemoveMessage(final RemoveMessage p_message) {
        while (!m_remover.push(p_message.getChunkIDs())) {
            m_logger.warn("Remover queue full, delaying remove and retry...");

            try {
                Thread.sleep(50);
            } catch (final InterruptedException ignored) {

            }
        }
    }

    /**
     * Handles an incoming ReuseIDMessage
     *
     * @param p_message
     *         the ReuseIDMessage
     */
    private void incomingReuseIDMessage(final ReuseIDMessage p_message) {
        for (long chunkID : p_message.getChunkIDs()) {
            m_chunk.getMemory().remove().prepareChunkIDForReuse(chunkID);
        }
    }

    /**
     * Separate remover thread to avoid blocking of message handlers
     */
    private class ChunkRemover extends Thread {
        private int m_queueMaxSize;
        private volatile boolean m_run = true;
        private ArrayDeque<long[]> m_queue = new ArrayDeque<>();
        private ReentrantLock m_lock = new ReentrantLock(false);
        private ReentrantLock m_condLock = new ReentrantLock(false);
        private Condition m_cond;

        /**
         * Constructor
         *
         * @param p_queueMaxSize
         *         Max queue size for remove jobs
         */
        public ChunkRemover(final int p_queueMaxSize) {
            m_queueMaxSize = p_queueMaxSize;
            m_cond = m_condLock.newCondition();
        }

        /**
         * Shut down the remover thread
         */
        public void shutdown() {
            m_run = false;

            m_condLock.lock();
            m_cond.signalAll();
            m_condLock.unlock();

            try {
                join();
            } catch (final InterruptedException ignored) {
            }
        }

        /**
         * Push one or multiple chunk IDs to the queue to schedule remove jobs
         *
         * @param p_chunkIds
         *         Chunk IDs to remove
         * @return True if pushing to queue successful, false if not enough space in queue to add all IDs
         */
        public boolean push(final long[] p_chunkIds) {
            boolean ret;

            m_lock.lock();

            if (m_queue.size() + 1 >= m_queueMaxSize) {
                m_cond.signalAll();
                return false;
            }

            ret = m_queue.offer(p_chunkIds);
            m_lock.unlock();

            m_condLock.lock();
            m_cond.signalAll();
            m_condLock.unlock();

            return ret;
        }

        @Override
        public void run() {
            while (m_run) {
                m_condLock.lock();

                try {
                    m_cond.await();
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }

                m_condLock.unlock();

                m_lock.lock();

                long[] elem;

                while (true) {
                    elem = m_queue.poll();

                    if (elem == null) {
                        break;
                    }

                    remove(elem);
                }

                m_lock.unlock();
            }
        }

        /**
         * Remove chunks denoted by a list of chunk IDs from the key-value store
         *
         * @param p_chunkIDs
         *         Chunk IDs of the chunks to remove
         */
        private void remove(final long[] p_chunkIDs) {
            int size;

            SOP_INCOMING_REMOVE.add(p_chunkIDs.length);
            SOP_INCOMING_REMOVE_TIME.start();

            Map<Long, ArrayListLong> remoteChunksByBackupPeers = new TreeMap<>();
            Map<Short, ArrayListLong> reuseChunkIDsByPeers = new TreeMap<>();

            // remove chunks from superpeer overlay first, so cannot be found before being deleted
            m_lookup.removeChunkIDs(ArrayListLong.wrap(p_chunkIDs));

            for (long p_chunkID : p_chunkIDs) {
                if (m_backup.isActive()) {
                    // sort by backup peers
                    long backupPeersAsLong = m_backup.getBackupPeersForLocalChunks(p_chunkID);
                    ArrayListLong remoteChunkIDsOfBackupPeers = remoteChunksByBackupPeers.computeIfAbsent(
                            backupPeersAsLong, k -> new ArrayListLong());
                    remoteChunkIDsOfBackupPeers.add(p_chunkID);
                }
            }

            // remove chunks first (local)
            for (long chunkID : p_chunkIDs) {
                size = m_chunk.getMemory().remove().remove(chunkID, false);

                if (size < 0) {
                    m_logger.warn("Removing chunk 0x%X failed: ", chunkID, ChunkState.values()[-size]);
                } else {
                    m_backup.deregisterChunk(chunkID, size);

                    if (ChunkID.getCreatorID(chunkID) != m_boot.getNodeId()) {
                        // sort by initial owner/creator for chunk ID reuse
                        ArrayListLong reuseChunkIDsOfPeer = reuseChunkIDsByPeers.computeIfAbsent(
                                ChunkID.getCreatorID(chunkID), a -> new ArrayListLong());
                        reuseChunkIDsOfPeer.add(chunkID);
                    }
                }
            }

            // send message to initial creator of locally stored but migrated removed chunks to allow re-use of chunk
            // ID, otherwise chunk ID gets lost here
            for (final Map.Entry<Short, ArrayListLong> reuseChunkIDs : reuseChunkIDsByPeers.entrySet()) {
                short peer = reuseChunkIDs.getKey();
                ArrayListLong chunkIDs = reuseChunkIDs.getValue();

                ReuseIDMessage message = new ReuseIDMessage(peer, chunkIDs);

                try {
                    m_network.sendMessage(message);
                } catch (final NetworkException e) {
                    m_logger.error("Sending reuse chunk ID message to peer 0x%X failed: %s", peer, e);
                }
            }

            // Inform backups
            if (m_backup.isActive()) {
                long backupPeersAsLong;
                BackupPeer[] backupPeers;
                ArrayListLong ids;

                for (Map.Entry<Long, ArrayListLong> entry : remoteChunksByBackupPeers.entrySet()) {
                    backupPeersAsLong = entry.getKey();
                    ids = entry.getValue();

                    backupPeers = BackupRange.convert(backupPeersAsLong);

                    for (BackupPeer backupPeer : backupPeers) {
                        if (backupPeer != null && backupPeer.getNodeID() != m_boot.getNodeId()) {

                            try {
                                m_network.sendMessage(
                                        new de.hhu.bsinfo.dxram.log.messages.RemoveMessage(
                                                backupPeer.getNodeID(), ids));
                            } catch (final NetworkException ignore) {

                            }
                        }
                    }
                }
            }

            SOP_INCOMING_REMOVE_TIME.stop();
        }
    }
}
