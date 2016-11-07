package de.hhu.bsinfo.dxram.migration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.log.messages.RemoveMessage;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.migration.messages.MigrationMessages;
import de.hhu.bsinfo.dxram.migration.messages.MigrationRemoteMessage;
import de.hhu.bsinfo.dxram.migration.messages.MigrationRequest;
import de.hhu.bsinfo.dxram.migration.messages.MigrationResponse;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;

/**
 * Migration service providing migration of chunks.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 30.03.2016
 */
public class MigrationService extends AbstractDXRAMService implements MessageReceiver {

    private static final Logger LOGGER = LogManager.getFormatterLogger(MigrationService.class.getSimpleName());

    // dependent components
    private AbstractBootComponent m_boot;
    private BackupComponent m_backup;
    private ChunkComponent m_chunk;
    private LookupComponent m_lookup;
    private MemoryManagerComponent m_memoryManager;
    private NetworkComponent m_network;

    private Lock m_migrationLock;

    /**
     * Creates an instance of MigrationService
     */
    public MigrationService() {
        super("migrate");
    }

    /**
     * Migrates the corresponding Chunk for the giving ID to another Node
     *
     * @param p_chunkID
     *     the ID
     * @param p_target
     *     the Node where to migrate the Chunk
     * @return true=success, false=failed
     */
    public boolean migrate(final long p_chunkID, final short p_target) {
        short[] backupPeers;
        Chunk chunk;
        boolean ret = false;

        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A superpeer must not store chunks");
            // #endif /* LOGGER >= ERROR */
        } else {
            m_migrationLock.lock();
            if (p_target != m_boot.getNodeID() && m_memoryManager.exists(p_chunkID)) {
                int size;

                m_memoryManager.lockAccess();
                size = m_memoryManager.getSize(p_chunkID);
                chunk = new Chunk(p_chunkID, size);
                m_memoryManager.get(chunk);
                m_memoryManager.unlockAccess();

                // #if LOGGER == TRACE
                LOGGER.trace("Sending migration request to %s", p_target);
                // #endif /* LOGGER == TRACE */

                MigrationRequest request = new MigrationRequest(p_target, new Chunk[] {chunk});
                try {
                    m_network.sendSync(request);
                } catch (final NetworkException e) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Could not migrate chunks");
                    // #endif /* LOGGER >= ERROR */
                    return false;
                }

                MigrationResponse response = (MigrationResponse) request.getResponse();
                if (response.getStatusCode() == -1) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Could not migrate chunks");
                    // #endif /* LOGGER >= ERROR */
                    return false;
                }

                // Update superpeers
                m_lookup.migrate(p_chunkID, p_target);

                // TODO:
                // Remove all locks
                // m_lock.unlockAll(p_chunkID);

                // Update local memory management
                m_memoryManager.remove(p_chunkID, true);
                if (m_backup.isActive()) {
                    // Update logging
                    backupPeers = m_backup.getCopyOfBackupPeersForLocalChunks(p_chunkID);
                    if (backupPeers != null) {
                        for (int i = 0; i < backupPeers.length; i++) {
                            if (backupPeers[i] != m_boot.getNodeID() && backupPeers[i] != -1) {
                                try {
                                    m_network.sendMessage(new RemoveMessage(backupPeers[i], new Long[] {p_chunkID}));
                                } catch (final NetworkException ignored) {

                                }
                            }
                        }
                    }
                }
                ret = true;
            } else {
                // #if LOGGER >= ERROR
                LOGGER.error("Chunk with ChunkID 0x%X could not be migrated", p_chunkID);
                // #endif /* LOGGER >= ERROR */
                ret = false;
            }
            m_migrationLock.unlock();
        }
        return ret;
    }

    /**
     * Triggers a migrate call to the node a specified chunk
     *
     * @param p_chunkID
     *     the ID
     * @param p_target
     *     the Node where to migrate the Chunk
     */
    public void targetMigrate(final long p_chunkID, final short p_target) {

        try {
            m_network.sendMessage(new MigrationRemoteMessage(ChunkID.getCreatorID(p_chunkID), p_chunkID, p_target));
        } catch (final NetworkException ignored) {

        }
        m_lookup.invalidate(p_chunkID);
    }

    /**
     * Migrates the corresponding Chunks for the giving ID range to another Node
     *
     * @param p_startChunkID
     *     the first ID
     * @param p_endChunkID
     *     the last ID
     * @param p_target
     *     the Node where to migrate the Chunks
     * @return true=success, false=failed
     */
    public boolean migrateRange(final long p_startChunkID, final long p_endChunkID, final short p_target) {
        long[] chunkIDs;
        short[] backupPeers;
        int counter;
        long iter;
        long size;
        Chunk chunk;
        Chunk[] chunks;
        boolean ret = false;

        // TODO: Handle range properly

        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A superpeer must not store chunks");
            // #endif /* LOGGER >= ERROR */
        } else {
            if (p_startChunkID <= p_endChunkID) {
                chunkIDs = new long[(int) (p_endChunkID - p_startChunkID + 1)];
                m_migrationLock.lock();
                if (p_target != m_boot.getNodeID()) {
                    iter = p_startChunkID;
                    while (true) {
                        // Send chunks to p_target
                        chunks = new Chunk[(int) (p_endChunkID - iter + 1)];
                        counter = 0;
                        size = 0;
                        m_memoryManager.lockAccess();
                        while (iter <= p_endChunkID) {
                            if (m_memoryManager.exists(iter)) {
                                int sizeChunk;

                                sizeChunk = m_memoryManager.getSize(iter);
                                chunk = new Chunk(iter, sizeChunk);
                                m_memoryManager.get(chunk);

                                chunks[counter] = chunk;
                                chunkIDs[counter++] = chunk.getID();
                                size += chunk.getDataSize();
                            } else {
                                // #if LOGGER >= ERROR
                                LOGGER.error("Chunk with ChunkID 0x%X could not be migrated", iter);
                                // #endif /* LOGGER >= ERROR */
                            }
                            iter++;
                        }
                        m_memoryManager.unlockAccess();

                        // #if LOGGER >= INFO
                        LOGGER.info("Sending %d Chunks (%d Bytes) to 0x%X", counter, size, p_target);
                        // #endif /* LOGGER >= INFO */
                        try {
                            m_network.sendSync(new MigrationRequest(p_target, Arrays.copyOf(chunks, counter)));
                        } catch (final NetworkException e) {
                            // #if LOGGER >= ERROR
                            LOGGER.error("Could not migrate chunks");
                            // #endif /* LOGGER >= ERROR */
                        }

                        if (iter > p_endChunkID) {
                            break;
                        }
                    }

                    // Update superpeers
                    m_lookup.migrateRange(p_startChunkID, p_endChunkID, p_target);

                    if (m_backup.isActive()) {
                        // Update logging
                        backupPeers = m_backup.getCopyOfBackupPeersForLocalChunks(iter);
                        if (backupPeers != null) {
                            for (int i = 0; i < backupPeers.length; i++) {
                                if (backupPeers[i] != m_boot.getNodeID() && backupPeers[i] != -1) {
                                    try {
                                        m_network.sendMessage(new RemoveMessage(backupPeers[i], chunkIDs));
                                    } catch (final NetworkException ignored) {

                                    }
                                }
                            }
                        }
                    }

                    iter = p_startChunkID;
                    while (iter <= p_endChunkID) {
                        // TODO:
                        // Remove all locks
                        // m_lock.unlockAll(iter);

                        // Update local memory management
                        m_memoryManager.remove(iter, true);
                        iter++;
                    }
                    ret = true;
                } else {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Chunks could not be migrated because end of range is before start of range");
                    // #endif /* LOGGER >= ERROR */
                    ret = false;
                }
            } else {
                // #if LOGGER >= ERROR
                LOGGER.error("Chunks could not be migrated");
                // #endif /* LOGGER >= ERROR */
                ret = false;
            }
            m_migrationLock.unlock();

            // #if LOGGER >= INFO
            LOGGER.info("All chunks migrated");
            // #endif /* LOGGER >= INFO */
        }
        return ret;
    }

    /**
     * Migrates all chunks to another node. Is called for promotion.
     *
     * @param p_target
     *     the peer that should take over all chunks
     */
    public void migrateAll(final short p_target) {
        long localID;
        long chunkID;
        long firstID;
        long lastID;
        ArrayList<Long> allMigratedChunks;
        Iterator<Long> iter;

        // Migrate all chunks created on this node
        final ArrayList<Long> ownChunkRanges = m_memoryManager.getCIDRangesOfAllLocalChunks();
        assert ownChunkRanges != null;
        for (int i = 0; i < ownChunkRanges.size(); i += 2) {
            firstID = ownChunkRanges.get(i);
            lastID = ownChunkRanges.get(i + 1);
            for (localID = firstID; localID < lastID; i++) {
                chunkID = ((long) m_boot.getNodeID() << 48) + localID;
                if (m_memoryManager.exists(chunkID)) {
                    migrate(chunkID, p_target);
                }
            }
        }

        // Migrate all chunks migrated to this node
        allMigratedChunks = m_memoryManager.getCIDOfAllMigratedChunks();
        if (allMigratedChunks != null) {
            iter = allMigratedChunks.iterator();
            while (iter.hasNext()) {
                chunkID = iter.next();
                migrate(chunkID, p_target);
            }
        }
    }

    @Override
    public void onIncomingMessage(final AbstractMessage p_message) {

        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE) {
                switch (p_message.getSubtype()) {
                    case MigrationMessages.SUBTYPE_MIGRATION_REQUEST:
                        incomingMigrationRequest((MigrationRequest) p_message);
                        break;
                    case MigrationMessages.SUBTYPE_MIGRATION_REMOTE_MESSAGE:
                        incomingMigrationMessage((MigrationRemoteMessage) p_message);
                        break;

                    default:
                        break;
                }
            }
        }
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_backup = p_componentAccessor.getComponent(BackupComponent.class);
        m_chunk = p_componentAccessor.getComponent(ChunkComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
        m_memoryManager = p_componentAccessor.getComponent(MemoryManagerComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        m_migrationLock = new ReentrantLock(false);

        registerNetworkMessages();
        registerNetworkMessageListener();

        return true;
    }

    /**
     * Handles an incoming MigrationRequest
     *
     * @param p_request
     *     the MigrationRequest
     */
    private void incomingMigrationRequest(final MigrationRequest p_request) {

        MigrationResponse response = new MigrationResponse(p_request);

        if (!m_chunk.putForeignChunks((Chunk[]) p_request.getDataStructures())) {
            response.setStatusCode((byte) -1);
        }

        try {
            m_network.sendMessage(response);
        } catch (final NetworkException ignored) {

        }
    }

    /**
     * Handles an incoming Remote Migratrion Request. E.g. a peer receives this message from a
     * terminal peer.
     *
     * @param p_message
     *     the message to trigger the Migration from another peer
     */
    private void incomingMigrationMessage(final MigrationRemoteMessage p_message) {
        // Outsource migration to another thread to avoid blocking a message handler
        Runnable task = () -> {
            boolean success = migrate(p_message.getChunkID(), p_message.getTargetNode());

            if (!success) {
                // #if LOGGER == TRACE
                LOGGER.trace("Failure! Could not migrate chunk 0x%X to node 0x%X", p_message.getChunkID(), p_message.getTargetNode());
                // #endif /* LOGGER == TRACE */
            }
        };
        new Thread(task).start();
    }

    // -----------------------------------------------------------------------------------

    /**
     * Register network messages we use in here.
     */
    private void registerNetworkMessages() {
        m_network.registerMessageType(DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE, MigrationMessages.SUBTYPE_MIGRATION_REQUEST, MigrationRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE, MigrationMessages.SUBTYPE_MIGRATION_RESPONSE, MigrationResponse.class);
        m_network
            .registerMessageType(DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE, MigrationMessages.SUBTYPE_MIGRATION_REMOTE_MESSAGE, MigrationRemoteMessage.class);

    }

    /**
     * Register network messages we want to listen to in here.
     */
    private void registerNetworkMessageListener() {
        m_network.register(MigrationRequest.class, this);
        m_network.register(MigrationRemoteMessage.class, this);
    }

}
