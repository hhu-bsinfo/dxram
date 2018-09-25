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

package de.hhu.bsinfo.dxram.log;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxlog.DXLog;
import de.hhu.bsinfo.dxlog.storage.header.AbstractSecLogEntryHeader;
import de.hhu.bsinfo.dxlog.storage.recovery.RecoveryMetadata;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxnet.core.MessageHeader;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;
import de.hhu.bsinfo.dxram.log.messages.InitBackupRangeRequest;
import de.hhu.bsinfo.dxram.log.messages.InitBackupRangeResponse;
import de.hhu.bsinfo.dxram.log.messages.InitRecoveredBackupRangeRequest;
import de.hhu.bsinfo.dxram.log.messages.InitRecoveredBackupRangeResponse;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;

/**
 * This service provides access to the backend storage system.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public final class LogComponent extends AbstractDXRAMComponent<LogComponentConfig> {

    // component dependencies
    private NetworkComponent m_network;
    private AbstractBootComponent m_boot;
    private BackupComponent m_backup;
    private ChunkComponent m_chunk;

    // private state
    private DXLog m_dxlog;
    private boolean m_loggingIsActive;

    /**
     * Creates the log component
     */
    public LogComponent() {
        super(DXRAMComponentOrder.Init.LOG, DXRAMComponentOrder.Shutdown.LOG, LogComponentConfig.class);
    }

    /**
     * Returns the header size
     *
     * @param p_nodeID
     *         the NodeID
     * @param p_localID
     *         the LocalID
     * @param p_size
     *         the size of the Chunk
     * @return the header size
     */
    public short getApproxHeaderSize(final short p_nodeID, final long p_localID, final int p_size) {
        return AbstractSecLogEntryHeader.getApproxSecLogHeaderSize(m_boot.getNodeId() != p_nodeID, p_localID, p_size);
    }

    /**
     * Initializes a new backup range
     *
     * @param p_backupRange
     *         the backup range
     */
    public void initBackupRange(final BackupRange p_backupRange) {
        BackupPeer[] backupPeers;
        InitBackupRangeRequest request;
        InitBackupRangeResponse response;
        long time;

        backupPeers = p_backupRange.getBackupPeers();

        time = System.currentTimeMillis();
        if (backupPeers != null) {
            for (int i = 0; i < backupPeers.length; i++) {
                if (backupPeers[i] != null) {
                    // The last peer is new in a recovered backup range
                    request = new InitBackupRangeRequest(backupPeers[i].getNodeID(), p_backupRange.getRangeID());

                    try {
                        m_network.sendSync(request);
                    } catch (final NetworkException ignore) {
                        i--;
                        continue;
                    }

                    response = request.getResponse(InitBackupRangeResponse.class);

                    if (!response.getStatus()) {
                        i--;
                    }
                }
            }
        }

        LOGGER.trace("Time to initialize range: %d", System.currentTimeMillis() - time);

    }

    /**
     * Initializes a recovered backup range
     *
     * @param p_backupRange
     *         the backup range
     * @param p_oldBackupRange
     *         the old backup range on the failed peer
     * @param p_failedPeer
     *         the failed peer
     */
    public void initRecoveredBackupRange(final BackupRange p_backupRange, final short p_oldBackupRange,
            final short p_failedPeer, final short p_newBackupPeer) {
        BackupPeer[] backupPeers = p_backupRange.getBackupPeers();
        if (backupPeers != null) {
            for (int i = 0; i < backupPeers.length; i++) {
                if (backupPeers[i] != null) {
                    if (backupPeers[i].getNodeID() == p_newBackupPeer) {
                        initBackupRangeOnPeer(backupPeers[i].getNodeID(), p_backupRange.getRangeID(), p_oldBackupRange,
                                p_failedPeer, true);
                    } else {
                        initBackupRangeOnPeer(backupPeers[i].getNodeID(), p_backupRange.getRangeID(), p_oldBackupRange,
                                p_failedPeer, false);
                    }
                }
            }
        }
    }

    /**
     * Initializes a new backup range
     *
     * @param p_backupPeer
     *         the backup peer
     * @param p_rangeID
     *         the new range ID
     * @param p_originalRangeID
     *         the old range ID
     * @param p_originalOwner
     *         the failed peer
     * @param p_isNewPeer
     *         whether this backup range is new for given backup peer or already stored for failed peer
     */
    private void initBackupRangeOnPeer(final short p_backupPeer, final short p_rangeID, final short p_originalRangeID,
            final short p_originalOwner, final boolean p_isNewPeer) {
        InitRecoveredBackupRangeRequest request;
        InitRecoveredBackupRangeResponse response;
        long time;

        time = System.currentTimeMillis();
        request = new InitRecoveredBackupRangeRequest(p_backupPeer, p_rangeID, p_originalRangeID, p_originalOwner,
                p_isNewPeer);
        try {
            m_network.sendSync(request);
        } catch (final NetworkException ignore) {
        }

        response = request.getResponse(InitRecoveredBackupRangeResponse.class);

        if (response == null || !response.getStatus()) {

            LOGGER.error("Backup range could not be initialized on 0x%X!", p_backupPeer);

        }

        LOGGER.trace("Time to initialize range: %d", System.currentTimeMillis() - time);

    }

    /**
     * Removes the logs and buffers from given backup range.
     *
     * @param p_owner
     *         the owner of the backup range
     * @param p_rangeID
     *         the RangeID
     */
    public void removeBackupRange(final short p_owner, final short p_rangeID) {
        m_dxlog.removeBackupRange(p_owner, p_rangeID);
    }

    /**
     * Recovers all Chunks of given backup range
     *
     * @param p_owner
     *         the NodeID of the node whose Chunks have to be restored
     * @param p_rangeID
     *         the RangeID
     * @return the recovery metadata
     */
    public RecoveryMetadata recoverBackupRange(final short p_owner, final short p_rangeID) {
        return m_dxlog.recoverBackupRange(p_owner, p_rangeID);
    }

    /**
     * Recovers all Chunks of given backup range
     *
     * @param p_fileName
     *         the file name
     * @param p_path
     *         the path of the folder the file is in
     * @return the recovered Chunks
     */
    public AbstractChunk[] recoverBackupRangeFromFile(final String p_fileName, final String p_path) {
        return m_dxlog.recoverBackupRangeFromFile(p_fileName, p_path);
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
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_backup = p_componentAccessor.getComponent(BackupComponent.class);
        m_chunk = p_componentAccessor.getComponent(ChunkComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.Config p_config, final DXRAMJNIManager p_jniManager) {

        m_loggingIsActive = m_boot.getNodeRole() == NodeRole.PEER && m_backup.isActiveAndAvailableForBackup();
        if (m_loggingIsActive) {
            m_dxlog = new DXLog(getConfig().getDxlogConfig(), m_boot.getNodeId(),
                    m_backup.getConfig().getBackupDirectory(),
                    (int) m_backup.getConfig().getBackupRangeSize().getBytes() * 2, m_chunk.getMemory().recovery());
        }

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        if (m_loggingIsActive) {
            m_dxlog.close();
        }

        return true;
    }

    /**
     * Initializes a new backup range
     *
     * @param p_rangeID
     *         the RangeID
     * @param p_owner
     *         the Chunks' owner
     * @return whether the operation was successful or not
     */
    boolean incomingInitBackupRange(final short p_rangeID, final short p_owner) {
        return m_dxlog.initBackupRange(p_rangeID, p_owner);
    }

    /**
     * Initializes a backup range after recovery (creating a new one or transferring the old)
     *
     * @param p_rangeID
     *         the RangeID
     * @param p_owner
     *         the Chunks' owner
     * @return whether the backup range is initialized or not
     */
    boolean incomingInitRecoveredBackupRange(final short p_rangeID, final short p_owner, final short p_originalRangeID,
            final short p_originalOwner, final boolean p_isNewBackupRange) {
        return m_dxlog
                .initRecoveredBackupRange(p_rangeID, p_owner, p_originalRangeID, p_originalOwner, p_isNewBackupRange);
    }

    /**
     * This is a special receiver message. To avoid creating and deserializing the message,
     * the message header is passed here directly (if complete, split messages are handled normally).
     *
     * @param p_messageHeader
     *         the message header (the payload is yet to be deserialized)
     */
    void incomingLogChunks(final MessageHeader p_messageHeader) {
        m_dxlog.logChunks(p_messageHeader);
    }

    /**
     * Logs a buffer with Chunks on SSD
     *
     * @param p_owner
     *         the Chunks' owner
     * @param p_rangeID
     *         the RangeID
     * @param p_numberOfDataStructures
     *         the number of data structures stored in p_buffer
     * @param p_buffer
     *         the Chunk buffer
     */
    void incomingLogChunks(final short p_owner, final short p_rangeID, final int p_numberOfDataStructures,
            final ByteBuffer p_buffer) {
        m_dxlog.logChunks(p_owner, p_rangeID, p_numberOfDataStructures, p_buffer);
    }

    /**
     * Removes Chunks from log
     *
     * @param p_rangeID
     *         the RangeID
     * @param p_owner
     *         the Chunks' owner
     * @param p_chunkIDs
     *         the ChunkIDs of all to be deleted chunks
     */

    void incomingRemoveChunks(final short p_rangeID, final short p_owner, final long[] p_chunkIDs) {
        m_dxlog.removeChunks(p_rangeID, p_owner, p_chunkIDs);
    }

    /**
     * Returns the current utilization of primary log and all secondary logs
     *
     * @return the current utilization
     */
    String getCurrentUtilization() {
        return m_dxlog.getCurrentUtilization();
    }

}
