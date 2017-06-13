package de.hhu.bsinfo.dxram.chunk;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.chunk.messages.RemoveMessage;
import de.hhu.bsinfo.dxram.chunk.messages.ReuseIDMessage;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.stats.StatisticsOperation;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorderManager;
import de.hhu.bsinfo.net.MessageReceiver;
import de.hhu.bsinfo.net.core.AbstractMessage;
import de.hhu.bsinfo.net.core.NetworkException;
import de.hhu.bsinfo.utils.ArrayListLong;
import de.hhu.bsinfo.utils.NodeID;

/**
 * This service provides access to the backend storage system (removals only)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 10.04.2017
 */
public class ChunkRemoveService extends AbstractDXRAMService<ChunkRemoveServiceConfig> implements MessageReceiver {
    // statistics recording
    private static final StatisticsOperation SOP_REMOVE = StatisticsRecorderManager.getOperation(ChunkService.class, "Remove");
    private static final StatisticsOperation SOP_INCOMING_REMOVE = StatisticsRecorderManager.getOperation(ChunkService.class, "IncomingRemove");

    // component dependencies
    private AbstractBootComponent m_boot;
    private BackupComponent m_backup;
    private MemoryManagerComponent m_memoryManager;
    private NetworkComponent m_network;
    private LookupComponent m_lookup;

    private ChunkRemover m_remover;

    /**
     * Constructor
     */
    public ChunkRemoveService() {
        super("chunkrem", ChunkRemoveServiceConfig.class);
    }

    /**
     * Remove chunks/data structures from the storage.
     *
     * @param p_dataStructures
     *         Data structures to remove from the storage.
     * @return Number of successfully removed data structures.
     */
    public int remove(final DataStructure... p_dataStructures) {
        long[] chunkIDs = new long[p_dataStructures.length];
        for (int i = 0; i < chunkIDs.length; i++) {
            chunkIDs[i] = p_dataStructures[i].getID();
        }

        return remove(chunkIDs);
    }

    /**
     * Remove chunks/data structures from the storage (by handle/ID).
     *
     * @param p_chunkIDs
     *         ChunkIDs/Handles of the data structures to remove. Invalid values are ignored.
     * @return Number of successfully removed data structures.
     */
    public int remove(final long... p_chunkIDs) {
        int chunksRemoved = 0;
        int size;

        if (p_chunkIDs.length == 0) {
            return chunksRemoved;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("remove[dataStructures(%d) %s, ...]", p_chunkIDs.length, ChunkID.toHexString(p_chunkIDs[0]));
        // #endif /* LOGGER == TRACE */

        // #ifdef STATISTICS
        SOP_REMOVE.enter(p_chunkIDs.length);
        // #endif /* STATISTICS */

        // sort by local and remote data first
        Map<Short, ArrayListLong> remoteChunksByPeers = new TreeMap<>();
        Map<Long, ArrayListLong> remoteChunksByBackupPeers = new TreeMap<>();
        ArrayListLong localChunks = new ArrayListLong();
        Map<Short, ArrayListLong> reuseChunkIDsByPeers = new TreeMap<>();

        try {
            m_memoryManager.lockAccess();
            for (int i = 0; i < p_chunkIDs.length; i++) {
                // invalid values allowed -> filter
                if (p_chunkIDs[i] == ChunkID.INVALID_ID) {
                    continue;
                }

                if (m_memoryManager.exists(p_chunkIDs[i])) {
                    if (ChunkID.getCreatorID(p_chunkIDs[i]) != m_boot.getNodeID()) {
                        // sort by initial owner/creator for chunk ID reuse
                        ArrayListLong reuseChunkIDsOfPeer = reuseChunkIDsByPeers.computeIfAbsent(ChunkID.getCreatorID(p_chunkIDs[i]), a -> new ArrayListLong());
                        reuseChunkIDsOfPeer.add(p_chunkIDs[i]);
                    }

                    // local and locally stored migrated chunks
                    localChunks.add(p_chunkIDs[i]);

                    if (m_backup.isActive()) {
                        // sort by backup peers
                        long backupPeersAsLong = m_backup.getBackupPeersForLocalChunks(p_chunkIDs[i]);
                        ArrayListLong remoteChunkIDsOfBackupPeers = remoteChunksByBackupPeers.computeIfAbsent(backupPeersAsLong, a -> new ArrayListLong());
                        remoteChunkIDsOfBackupPeers.add(p_chunkIDs[i]);
                    }
                } else {
                    // remote chunks, figure out location and sort by peers
                    LookupRange lookupRange;

                    lookupRange = m_lookup.getLookupRange(p_chunkIDs[i]);
                    if (lookupRange != null) {
                        short peer = lookupRange.getPrimaryPeer();

                        ArrayListLong remoteChunksOfPeer = remoteChunksByPeers.computeIfAbsent(peer, a -> new ArrayListLong());
                        remoteChunksOfPeer.add(p_chunkIDs[i]);
                    }
                }
            }
        } finally {
            m_memoryManager.unlockAccess();
        }

        // remove local chunks from superpeer overlay first, so cannot be found before being deleted
        m_lookup.removeChunkIDs(localChunks);

        try {
            // remove local chunkIDs
            m_memoryManager.lockManage();
            for (int i = 0; i < localChunks.getSize(); i++) {
                size = m_memoryManager.remove(localChunks.get(i), false);
                if (size > 0) {
                    chunksRemoved++;
                    m_backup.deregisterChunk(localChunks.get(i), size);
                } else {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Removing chunk ID 0x%X failed, does not exist", localChunks.get(i));
                    // #endif /* LOGGER >= ERROR */
                }
            }
        } finally {
            m_memoryManager.unlockManage();
        }

        // send message to initial creator of locally stored but migrated removed chunks to allow re-use of chunk ID, otherwise chunk ID gets lost here
        for (final Map.Entry<Short, ArrayListLong> reuseChunkIDs : reuseChunkIDsByPeers.entrySet()) {
            short peer = reuseChunkIDs.getKey();
            ArrayListLong chunkIDs = reuseChunkIDs.getValue();

            ReuseIDMessage message = new ReuseIDMessage(peer, chunkIDs);

            try {
                m_network.sendMessage(message);
            } catch (final NetworkException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Sending reuse chunk ID message to peer 0x%X failed: %s", peer, e);
                // #endif /* LOGGER >= ERROR */
            }
        }

        // go for remote ones by each peer
        for (final Map.Entry<Short, ArrayListLong> peerWithChunks : remoteChunksByPeers.entrySet()) {
            short peer = peerWithChunks.getKey();
            ArrayListLong remoteChunks = peerWithChunks.getValue();

            if (peer == m_boot.getNodeID()) {
                // local remove, migrated data to current node
                // remove migrated chunks from superpeer overlay first, so cannot be found before being deleted
                m_lookup.removeChunkIDs(remoteChunks);

                try {
                    m_memoryManager.lockManage();
                    for (int i = 0; i < remoteChunks.getSize(); i++) {
                        size = m_memoryManager.remove(localChunks.get(i), false);
                        if (size > 0) {
                            chunksRemoved++;
                            m_backup.deregisterChunk(localChunks.get(i), size);
                        } else {
                            // #if LOGGER >= ERROR
                            LOGGER.error("Removing chunk ID 0x%X failed, does not exist", remoteChunks.get(i));
                            // #endif /* LOGGER >= ERROR */
                        }
                    }
                } finally {
                    m_memoryManager.unlockManage();
                }
            } else {
                // Remote remove from specified peer
                RemoveMessage message = new RemoveMessage(peer, remoteChunks);
                try {
                    m_network.sendMessage(message);
                } catch (final NetworkException e) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Sending chunk remove to peer 0x%X failed: %s", peer, e);
                    // #endif /* LOGGER >= ERROR */
                    continue;
                }

                chunksRemoved += remoteChunks.getSize();
            }
        }

        // Inform backups
        if (m_backup.isActive()) {
            long backupPeersAsLong;
            short[] backupPeers;
            ArrayListLong ids;
            for (Map.Entry<Long, ArrayListLong> entry : remoteChunksByBackupPeers.entrySet()) {
                backupPeersAsLong = entry.getKey();
                ids = entry.getValue();

                backupPeers = BackupRange.convert(backupPeersAsLong);
                for (int i = 0; i < backupPeers.length; i++) {
                    if (backupPeers[i] != m_boot.getNodeID() && backupPeers[i] != NodeID.INVALID_ID) {
                        try {
                            m_network.sendMessage(new de.hhu.bsinfo.dxram.log.messages.RemoveMessage(backupPeers[i], ids));
                        } catch (final NetworkException ignore) {

                        }
                    }
                }
            }
        }

        // #ifdef STATISTICS
        SOP_REMOVE.leave();
        // #endif /* STATISTICS */

        // #if LOGGER == TRACE
        LOGGER.trace("remove[dataStructures(%d) 0x%X, ...] -> %d", p_chunkIDs.length, p_chunkIDs[0], chunksRemoved);
        // #endif /* LOGGER == TRACE */

        return chunksRemoved;
    }

    @Override
    public void onIncomingMessage(final AbstractMessage p_message) {
        // #if LOGGER == TRACE
        LOGGER.trace("Entering incomingMessage with: p_message=%s", p_message);
        // #endif /* LOGGER == TRACE */

        if (p_message != null) {
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

        // #if LOGGER == TRACE
        LOGGER.trace("Exiting incomingMessage");
        // #endif /* LOGGER == TRACE */
    }

    @Override
    protected boolean supportsSuperpeer() {
        return false;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_backup = p_componentAccessor.getComponent(BackupComponent.class);
        m_memoryManager = p_componentAccessor.getComponent(MemoryManagerComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.Config p_config) {
        m_remover = new ChunkRemover(getConfig().getRemoverQueueSize());
        m_remover.start();

        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_REMOVE_MESSAGE, RemoveMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_REUSE_ID_MESSAGE, ReuseIDMessage.class);

        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_REMOVE_MESSAGE, this);
        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_REUSE_ID_MESSAGE, this);

        return true;
    }

    @Override
    protected boolean shutdownService() {
        m_remover.shutdown();
        m_remover = null;

        return true;
    }

    /**
     * Handles an incoming RemoveMessage
     *
     * @param p_message
     *         the RemoveMessage
     */
    private void incomingRemoveMessage(final RemoveMessage p_message) {
        while (!m_remover.push(p_message.getChunkIDs())) {
            // #if LOGGER == WARN
            LOGGER.warn("Remover queue full, delaying remove and retry...");
            // #endif /* LOGGER == WARN */

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
        try {
            m_memoryManager.lockAccess();

            for (long chunkID : p_message.getChunkIDs()) {
                m_memoryManager.prepareChunkIDForReuse(chunkID);
            }
        } finally {
            m_memoryManager.unlockAccess();
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

            // #ifdef STATISTICS
            SOP_INCOMING_REMOVE.enter(p_chunkIDs.length);
            // #endif /* STATISTICS */

            Map<Long, ArrayListLong> remoteChunksByBackupPeers = new TreeMap<>();
            Map<Short, ArrayListLong> reuseChunkIDsByPeers = new TreeMap<>();

            // remove chunks from superpeer overlay first, so cannot be found before being deleted
            m_lookup.removeChunkIDs(ArrayListLong.wrap(p_chunkIDs));

            for (int i = 0; i < p_chunkIDs.length; i++) {
                if (m_backup.isActive()) {
                    // sort by backup peers
                    long backupPeersAsLong = m_backup.getBackupPeersForLocalChunks(p_chunkIDs[i]);
                    ArrayListLong remoteChunkIDsOfBackupPeers = remoteChunksByBackupPeers.computeIfAbsent(backupPeersAsLong, k -> new ArrayListLong());
                    remoteChunkIDsOfBackupPeers.add(p_chunkIDs[i]);
                }
            }

            // remove chunks first (local)
            try {
                m_memoryManager.lockManage();
                for (int i = 0; i < p_chunkIDs.length; i++) {
                    size = m_memoryManager.remove(p_chunkIDs[i], false);
                    if (size == -1) {
                        // #if LOGGER >= ERROR
                        LOGGER.warn("Removing chunk 0x%X failed, does not exist", p_chunkIDs[i]);
                        // #endif /* LOGGER >= ERROR */
                    } else {
                        m_backup.deregisterChunk(p_chunkIDs[i], size);

                        if (ChunkID.getCreatorID(p_chunkIDs[i]) != m_boot.getNodeID()) {
                            // sort by initial owner/creator for chunk ID reuse
                            ArrayListLong reuseChunkIDsOfPeer =
                                    reuseChunkIDsByPeers.computeIfAbsent(ChunkID.getCreatorID(p_chunkIDs[i]), a -> new ArrayListLong());
                            reuseChunkIDsOfPeer.add(p_chunkIDs[i]);
                        }
                    }
                }
            } finally {
                m_memoryManager.unlockManage();
            }

            // send message to initial creator of locally stored but migrated removed chunks to allow re-use of chunk ID, otherwise chunk ID gets lost here
            for (final Map.Entry<Short, ArrayListLong> reuseChunkIDs : reuseChunkIDsByPeers.entrySet()) {
                short peer = reuseChunkIDs.getKey();
                ArrayListLong chunkIDs = reuseChunkIDs.getValue();

                ReuseIDMessage message = new ReuseIDMessage(peer, chunkIDs);

                try {
                    m_network.sendMessage(message);
                } catch (final NetworkException e) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Sending reuse chunk ID message to peer 0x%X failed: %s", peer, e);
                    // #endif /* LOGGER >= ERROR */
                }
            }

            // Inform backups
            if (m_backup.isActive()) {
                long backupPeersAsLong;
                short[] backupPeers;
                ArrayListLong ids;
                for (Map.Entry<Long, ArrayListLong> entry : remoteChunksByBackupPeers.entrySet()) {
                    backupPeersAsLong = entry.getKey();
                    ids = entry.getValue();

                    backupPeers = BackupRange.convert(backupPeersAsLong);
                    for (int i = 0; i < backupPeers.length; i++) {
                        if (backupPeers[i] != m_boot.getNodeID() && backupPeers[i] != NodeID.INVALID_ID) {
                            try {
                                m_network.sendMessage(new de.hhu.bsinfo.dxram.log.messages.RemoveMessage(backupPeers[i], ids));
                            } catch (final NetworkException ignore) {

                            }
                        }
                    }
                }
            }

            // #ifdef STATISTICS
            SOP_INCOMING_REMOVE.leave();
            // #endif /* STATISTICS */
        }
    }
}
