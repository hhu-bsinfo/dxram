/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.migration;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkMigrationComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.log.messages.RemoveMessage;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.migration.messages.MigrationMessages;
import de.hhu.bsinfo.dxram.migration.messages.MigrationRemoteMessage;
import de.hhu.bsinfo.dxram.migration.messages.MigrationRequest;
import de.hhu.bsinfo.dxram.migration.messages.MigrationResponse;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.ArrayListLong;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Migration service providing migration of chunks.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 30.03.2016
 * @author Filip Krakowski, Filip.Krakowski@Uni-Duesseldorf.de, 12.06.2018
 */
public class MigrationService extends AbstractDXRAMService<MigrationServiceConfig> implements MessageReceiver {
    // component dependencies
    private AbstractBootComponent m_boot;
    private BackupComponent m_backup;
    private ChunkComponent m_chunk;
    private ChunkMigrationComponent m_chunkMigration;
    private LookupComponent m_lookup;
    private NetworkComponent m_network;

    private Lock m_migrationLock;

    private MigrationManager m_migrationManager = null;

    /**
     * Creates an instance of MigrationService
     */
    public MigrationService() {
        super("migrate", MigrationServiceConfig.class);
    }

    /**
     * Migrates a single chunk to another node.
     *
     * @param p_chunkID The chunk id.
     * @param p_target The target's node id.
     * @return true, if the chunk was migrated; false else
     */
    public boolean migrate(final long p_chunkID, final short p_target) {
        short[] backupPeers;
        boolean ret;

        m_migrationLock.lock();
        if (p_target != m_boot.getNodeId() && m_chunk.getMemory().exists().exists(p_chunkID)) {
            ChunkByteArray chunk = m_chunk.getMemory().get().get(p_chunkID, ChunkLockOperation.NONE, -1);

            LOGGER.trace("Sending migration request to %s", p_target);

            MigrationRequest request = new MigrationRequest(p_target, chunk);
            try {
                m_network.sendSync(request);
            } catch (final NetworkException e) {

                LOGGER.error("Could not migrate chunks");

                return false;
            }

            MigrationResponse response = (MigrationResponse) request.getResponse();
            if (response.getStatus() == -1) {

                LOGGER.error("Could not migrate chunks");

                return false;
            }

            // Update superpeers
            m_lookup.migrate(p_chunkID, p_target);

            // TODO: Remove all locks
            // m_lock.unlockAll(p_chunkID);

            // Update local memory management
            m_chunk.getMemory().remove().remove(p_chunkID, true);
            m_backup.deregisterChunk(p_chunkID, chunk.sizeofObject());

            if (m_backup.isActive()) {
                // Update logging
                backupPeers = m_backup.getArrayOfBackupPeersForLocalChunks(p_chunkID);
                if (backupPeers != null) {
                    for (int i = 0; i < backupPeers.length; i++) {
                        if (backupPeers[i] != m_boot.getNodeId() && backupPeers[i] != NodeID.INVALID_ID) {
                            try {
                                m_network.sendMessage(new RemoveMessage(backupPeers[i], new ArrayListLong(p_chunkID)));
                            } catch (final NetworkException ignored) {

                            }
                        }
                    }
                }
            }
            ret = true;
        } else {

            LOGGER.error("Chunk with ChunkID 0x%X could not be migrated", p_chunkID);

            ret = false;
        }
        m_migrationLock.unlock();

        return ret;
    }

    /**
     * Triggers a migrate call on the specified node.
     *
     * @param p_chunkID The chunk id.
     * @param p_target
     *         the Node where to migrate the Chunk
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
     * @param p_startChunkID The first chunk id
     * @param p_endChunkID The last chunk id
     * @param p_target The target node
     */
    public CompletableFuture<MigrationStatus> migrateRange(final long p_startChunkID, final long p_endChunkID, final short p_target) {
        return m_migrationManager.migrateRange(p_target, new LongRange(p_startChunkID, p_endChunkID));
    }

    /**
     * Migrates all chunks to another node.
     *
     * @param p_target
     *         the peer that should take over all chunks
     */
    public void migrateAll(final short p_target) {
        long localID;
        long chunkID;
        long firstID;
        long lastID;
        ChunkIDRanges allMigratedChunks;

        // Migrate all chunks created on this node
        ChunkIDRanges ownChunkRanges = m_chunk.getMemory().cidStatus().getCIDRangesOfLocalChunks();
        assert ownChunkRanges != null;

        for (int i = 0; i < ownChunkRanges.size(); i++) {
            firstID = ownChunkRanges.getRangeStart(i);
            lastID = ownChunkRanges.getRangeEnd(i);

            for (localID = firstID; localID < lastID; i++) {
                chunkID = ((long) m_boot.getNodeId() << 48) + localID;

                if (m_chunk.getMemory().exists().exists(chunkID)) {
                    migrate(chunkID, p_target);
                }
            }
        }

        // Migrate all chunks migrated to this node
        allMigratedChunks = m_chunk.getMemory().cidStatus().getCIDRangesOfLocalChunks();

        for (int i = 0; i < allMigratedChunks.size(); i++) {
            long rangeStart = allMigratedChunks.getRangeStart(i);
            long rangeEnd = allMigratedChunks.getRangeEnd(i);

            for (long chunkId = rangeStart; chunkId <= rangeEnd; chunkId++) {
                migrate(chunkId, p_target);
            }
        }
    }

    /**
     * Returns the number of active worker threads.
     *
     * @return The number of active worker threads.
     */
    public int getWorkerCount() {
        return m_migrationManager.getWorkerCount();
    }

    @Override
    public void onIncomingMessage(final Message p_message) {

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
    protected boolean supportsSuperpeer() {
        return false;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_migrationManager = new MigrationManager(1, p_componentAccessor);
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_backup = p_componentAccessor.getComponent(BackupComponent.class);
        m_chunk = p_componentAccessor.getComponent(ChunkComponent.class);
        m_chunkMigration = p_componentAccessor.getComponent(ChunkMigrationComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.Config p_config) {
        m_migrationLock = new ReentrantLock(false);

        registerNetworkMessages();
        registerNetworkMessageListener();

        return true;
    }

    /**
     * Handles an incoming MigrationRequest
     *
     * @param p_request
     *         the MigrationRequest
     */
    private void incomingMigrationRequest(final MigrationRequest p_request) {

        MigrationResponse response;
        if (!m_chunkMigration.putMigratedChunks(p_request.getChunkIDs(), p_request.getChunkData())) {
            response = new MigrationResponse(p_request, (byte) -1);
        } else {
            response = new MigrationResponse(p_request, (byte) 0);
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
     *         the message to trigger the Migration from another peer
     */
    private void incomingMigrationMessage(final MigrationRemoteMessage p_message) {
        // Outsource migration to another thread to avoid blocking a message handler
        Runnable task = () -> {
            boolean success = migrate(p_message.getChunkID(), p_message.getTargetNode());

            if (!success) {
                LOGGER.trace("Failure! Could not migrate chunk 0x%X to node 0x%X", p_message.getChunkID(),
                        p_message.getTargetNode());
            }
        };
        new Thread(task).start();
    }

    // -----------------------------------------------------------------------------------

    /**
     * Register network messages we use in here.
     */
    private void registerNetworkMessages() {
        m_migrationManager.registerMessages();
        m_network.registerMessageType(DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE,
                MigrationMessages.SUBTYPE_MIGRATION_REQUEST, MigrationRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE,
                MigrationMessages.SUBTYPE_MIGRATION_RESPONSE, MigrationResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE,
                MigrationMessages.SUBTYPE_MIGRATION_REMOTE_MESSAGE,
                MigrationRemoteMessage.class);

    }

    /**
     * Register network messages we want to listen to in here.
     */
    private void registerNetworkMessageListener() {
        m_network.register(DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE, MigrationMessages.SUBTYPE_MIGRATION_REQUEST,
                this);
        m_network.register(DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE,
                MigrationMessages.SUBTYPE_MIGRATION_REMOTE_MESSAGE, this);
    }

}
