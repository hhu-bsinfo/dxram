/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.backup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkBackupComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.event.AbstractEvent;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.event.EventListener;
import de.hhu.bsinfo.dxram.failure.events.NodeFailureEvent;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.messages.InitBackupRangeRequest;
import de.hhu.bsinfo.dxram.log.messages.InitBackupRangeResponse;
import de.hhu.bsinfo.dxram.log.messages.InitRecoveredBackupRangeRequest;
import de.hhu.bsinfo.dxram.log.messages.InitRecoveredBackupRangeResponse;
import de.hhu.bsinfo.dxram.log.messages.LogMessages;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.events.NodeJoinEvent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.recovery.RecoveryMetadata;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.utils.NodeID;

/**
 * Component for managing backup ranges.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 30.03.2016
 */
public class BackupComponent extends AbstractDXRAMComponent<BackupComponentConfig> implements EventListener<AbstractEvent> {

    private static final boolean REREPLICATION_ACTIVE = true;

    // component dependencies
    private AbstractBootComponent m_boot;
    private ChunkBackupComponent m_chunkBackup;
    private LookupComponent m_lookup;
    private LogComponent m_log;
    private EventComponent m_event;
    private NetworkComponent m_network;

    // private state
    private short m_nodeID;
    private long m_currentLocalID = -1;

    // All backup ranges for locally created, to this peer migrated and from this peer recovered chunks
    private ArrayList<BackupRange> m_backupRanges;

    // Every chunk must be registered here for getting the RangeID
    // Input: ChunkID; Output: backup range (RangeID)
    private BackupRangeTree m_backupRangeTree;

    // Current backup range
    private BackupRange m_currentBackupRange;

    private ArrayList<Short> m_firstBackupPeers;

    private ReentrantReadWriteLock m_lock;

    /**
     * Creates the backup component
     */
    public BackupComponent() {
        super(DXRAMComponentOrder.Init.BACKUP, DXRAMComponentOrder.Shutdown.BACKUP, BackupComponentConfig.class);
    }

    /**
     * Returns whether backup is enabled or not
     *
     * @return whether backup is enabled or not
     */
    public boolean isActive() {
        return getConfig().isBackupActive();
    }

    /**
     * Returns whether backup is enabled and this peer is used for logging/recovery or not
     *
     * @return whether backup is enabled and this peer is used for logging/recovery or not
     */
    public boolean isActiveAndAvailableForBackup() {
        return getConfig().isBackupActive() && getConfig().availableForBackup();
    }

    /**
     * Pass through for segment size of log
     *
     * @return Segment size of log in bytes
     */
    public int getLogSegmentSizeBytes() {
        return m_log.getSegmentSizeBytes();
    }

    /**
     * Registers a chunk in a backup range. Creates a new backup range if necessary.
     *
     * @param p_chunkID
     *         the ChunkID
     * @param p_size
     *         the size
     * @lock MemoryManager must be write locked
     */
    public BackupRange registerChunk(final long p_chunkID, final int p_size) {
        if (getConfig().isBackupActive() && p_chunkID != ChunkID.INVALID_ID) {
            return registerValidChunk(p_chunkID, p_size);
        } else {
            return null;
        }
    }

    /**
     * Registers a chunk in a backup range. Creates a new backup range if necessary.
     *
     * @param p_dataStructure
     *         the DataStructure
     * @return the corresponding backup range
     * @lock MemoryManager must be write locked
     */
    public BackupRange registerChunk(final DataStructure p_dataStructure) {
        if (getConfig().isBackupActive() && p_dataStructure != null && p_dataStructure.getID() != ChunkID.INVALID_ID) {
            return registerValidChunk(p_dataStructure);
        } else {
            return null;
        }
    }

    /**
     * Registers chunks in a backup range. Creates new backup ranges if necessary.
     *
     * @param p_dataStructures
     *         the data structures
     * @lock MemoryManager must be write locked
     */
    public void registerChunks(final DataStructure... p_dataStructures) {
        if (getConfig().isBackupActive()) {
            for (DataStructure dataStructure : p_dataStructures) {
                if (dataStructure.getID() != ChunkID.INVALID_ID) {
                    registerValidChunk(dataStructure.getID(), dataStructure.sizeofObject());
                }
            }
        }
    }

    /**
     * Registers chunks in a backup range. Creates new backup ranges if necessary.
     *
     * @param p_chunkIDs
     *         the ChunkIDs
     * @param p_size
     *         every chunks' size
     * @lock MemoryManager must be write locked
     */
    public void registerChunks(final long[] p_chunkIDs, final int p_size) {
        if (getConfig().isBackupActive()) {
            for (long chunkID : p_chunkIDs) {
                if (chunkID != ChunkID.INVALID_ID) {
                    registerValidChunk(chunkID, p_size);
                }
            }
        }
    }

    /**
     * Registers chunks in a backup range. Creates new backup ranges if necessary.
     *
     * @param p_chunkIDs
     *         the ChunkIDs
     * @param p_sizes
     *         the chunk sizes
     * @lock MemoryManager must be write locked
     */
    public void registerChunks(final long[] p_chunkIDs, final int[] p_sizes) {
        if (getConfig().isBackupActive()) {
            for (int i = 0; i < p_chunkIDs.length; i++) {
                if (p_chunkIDs[i] != ChunkID.INVALID_ID) {
                    registerValidChunk(p_chunkIDs[i], p_sizes[i]);
                }
            }
        }
    }

    /**
     * Register recovered chunks that were migrated to failed peer
     *
     * @param p_recoveryMetadata
     *         the ChunkIDs of all recovered chunks, number of recovered chunks and bytes
     * @param p_backupRange
     *         the recovered backup range
     * @param p_failedPeer
     *         the failed peer
     * @return the replacement backup peer
     */
    public short registerRecoveredChunks(final RecoveryMetadata p_recoveryMetadata, final BackupRange p_backupRange, final short p_failedPeer) {
        short ret;
        final short oldBackupRange = p_backupRange.getRangeID();

        // Create a new backup range for recovered backup range; use two old backup peers
        m_lock.writeLock().lock();
        if (REREPLICATION_ACTIVE) {
            ret = determineReplacementBackupPeer(p_backupRange.getBackupPeers());
        } else {
            ret = NodeID.INVALID_ID;
        }
        p_backupRange.replaceBackupPeer(m_nodeID, ret);
        p_backupRange.addChunks(p_recoveryMetadata.getSizeInBytes());
        p_backupRange.setRangeID((short) m_backupRanges.size());

        m_backupRanges.add(p_backupRange);
        m_lookup.initRange(p_backupRange);
        m_log.initRecoveredBackupRange(p_backupRange, oldBackupRange, p_failedPeer, ret);

        int counter = 1;
        for (short backupPeer : p_backupRange.getBackupPeers()) {
            if (backupPeer != NodeID.INVALID_ID) {
                // #if LOGGER >= INFO
                LOGGER.info("%d. backup peer determined for recovered range %d of 0x%X (now: %d on 0x%X): 0x%X", counter++, oldBackupRange, p_failedPeer,
                        p_backupRange.getRangeID(), m_nodeID, backupPeer);
                // #endif /* LOGGER >= INFO */
            }
        }

        // Register chunks in tree
        long[] chunkIDRanges = p_recoveryMetadata.getCIDRanges();
        for (int i = 0; i < chunkIDRanges.length; i += 2) {
            m_backupRangeTree.putChunkIDRange(chunkIDRanges[i], chunkIDRanges[i + 1], p_backupRange.getRangeID());
        }
        m_lock.writeLock().unlock();

        return ret;
    }

    /**
     * Deregisters a chunk from a backup range. Reduces the size of the backup range, only.
     *
     * @param p_chunkID
     *         the ChunkID
     * @param p_size
     *         the size
     * @lock MemoryManager must be write locked
     */
    public void deregisterChunk(final long p_chunkID, final int p_size) {
        short rangeID;
        int size;
        BackupRange backupRange;

        if (getConfig().isBackupActive() && p_chunkID != ChunkID.INVALID_ID) {
            m_lock.writeLock().lock();
            rangeID = m_backupRangeTree.getBackupRange(p_chunkID);
            backupRange = m_backupRanges.get(rangeID);
            size = p_size + m_log.getApproxHeaderSize(ChunkID.getCreatorID(p_chunkID), ChunkID.getLocalID(p_chunkID), p_size);

            backupRange.removeChunk(size);
            m_lock.writeLock().unlock();
        }
    }

    /**
     * Returns the corresponding backup range
     *
     * @param p_chunkID
     *         the ChunkID
     * @return the backup range
     */

    public BackupRange getBackupRange(final long p_chunkID) {
        BackupRange ret = null;
        short rangeID;

        m_lock.readLock().lock();
        rangeID = m_backupRangeTree.getBackupRange(p_chunkID);
        if (rangeID != RangeID.INVALID_ID) {
            ret = m_backupRanges.get(rangeID);
        } else {
            // #if LOGGER >= ERROR
            LOGGER.error("Backup range for 0x%X is unknown!", p_chunkID);
            // #endif /* LOGGER >= ERROR */
        }
        m_lock.readLock().unlock();

        return ret;
    }

    /**
     * Returns the corresponding backup peers
     *
     * @param p_chunkID
     *         the ChunkID
     * @return the backup peers
     */

    public long getBackupPeersForLocalChunks(final long p_chunkID) {
        long ret;
        short rangeID;

        m_lock.readLock().lock();
        rangeID = m_backupRangeTree.getBackupRange(p_chunkID);
        ret = m_backupRanges.get(rangeID).getBackupPeersAsLong();
        m_lock.readLock().unlock();

        return ret;
    }

    /**
     * Returns the corresponding backup peers
     *
     * @param p_chunkID
     *         the ChunkID
     * @return the backup peers
     */
    public short[] getArrayOfBackupPeersForLocalChunks(final long p_chunkID) {
        short[] ret;
        short rangeID;

        m_lock.readLock().lock();
        rangeID = m_backupRangeTree.getBackupRange(p_chunkID);
        ret = m_backupRanges.get(rangeID).getCopyOfBackupPeers();
        m_lock.readLock().unlock();

        return ret;
    }

    @Override
    public void eventTriggered(final AbstractEvent p_event) {
        short currentBackupPeer;
        short newBackupPeer;
        short rangeID;
        short[] backupPeers;

        // Both events can be triggered if backup is active, only
        if (p_event instanceof NodeFailureEvent) {
            NodeFailureEvent event = (NodeFailureEvent) p_event;
            short failedPeer = event.getNodeID();

            BackupRange currentBackupRange;
            // Replace failed peer in all backup ranges
            for (int i = 0; i < m_backupRanges.size(); i++) {
                m_lock.writeLock().lock();
                currentBackupRange = m_backupRanges.get(i);
                backupPeers = currentBackupRange.getCopyOfBackupPeers();
                rangeID = currentBackupRange.getRangeID();

                for (int j = 0; j < backupPeers.length; j++) {
                    currentBackupPeer = backupPeers[j];
                    if (currentBackupPeer == failedPeer) {
                        if (REREPLICATION_ACTIVE) {
                            // Determine new backup peer and replace it in backup range
                            newBackupPeer = determineReplacementBackupPeer(backupPeers);

                            currentBackupRange.replaceBackupPeer(failedPeer, newBackupPeer);
                            m_lock.writeLock().unlock();

                            // Send new backup peer all chunks of backup range
                            if (newBackupPeer != NodeID.INVALID_ID) {
                                m_chunkBackup.replicateBackupRange(newBackupPeer, m_backupRangeTree.getAllChunkIDRangesOfBackupRange(rangeID), rangeID);
                            }
                        } else {
                            newBackupPeer = NodeID.INVALID_ID;
                            currentBackupRange.replaceBackupPeer(failedPeer, newBackupPeer);
                            m_lock.writeLock().unlock();
                        }

                        // Inform responsible superpeer to update backup range
                        m_lookup.replaceBackupPeer(rangeID, failedPeer, newBackupPeer);

                        break;
                    }
                    if (j == backupPeers.length - 1) {
                        // Failed peer was not responsible for backup range
                        m_lock.writeLock().unlock();
                    }
                }
            }
        } else {
            NodeJoinEvent event = (NodeJoinEvent) p_event;
            short joinedPeer = event.getNodeID();

            List<Short> peers = m_boot.getIDsOfAvailableBackupPeers();
            for (short peer : peers) {
                if (peer == joinedPeer) {
                    BackupRange currentBackupRange;
                    // Search for backup ranges with insufficient backup peers
                    for (int i = 0; i < m_backupRanges.size(); i++) {
                        currentBackupRange = m_backupRanges.get(i);
                        rangeID = currentBackupRange.getRangeID();

                        m_lock.writeLock().lock();
                        if (currentBackupRange.addBackupPeer(joinedPeer)) {
                            m_lock.writeLock().unlock();
                            // Inform responsible superpeer to update backup range
                            m_lookup.replaceBackupPeer(rangeID, NodeID.INVALID_ID, joinedPeer);

                            // #if LOGGER >= INFO
                            LOGGER.info("Replicating backup range %d to new peer 0x%X", i, joinedPeer);
                            // #endif /* LOGGER >= INFO */

                            // Backup range was not complete -> send all chunks to joined peer
                            int num = m_chunkBackup.replicateBackupRange(joinedPeer, m_backupRangeTree.getAllChunkIDRangesOfBackupRange(rangeID), rangeID);

                            // #if LOGGER >= INFO
                            LOGGER.info("Replicated %d chunk(s) of backup range %d to new peer 0x%X", num, i, joinedPeer);
                            // #endif /* LOGGER >= INFO */
                        } else {
                            m_lock.writeLock().unlock();
                        }
                    }

                    break;
                }
            }
        }
    }

    @Override
    protected boolean supportsSuperpeer() {
        return true;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_chunkBackup = p_componentAccessor.getComponent(ChunkBackupComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
        m_log = p_componentAccessor.getComponent(LogComponent.class);
        m_event = p_componentAccessor.getComponent(EventComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.Config p_config) {
        m_nodeID = m_boot.getNodeID();

        if (m_boot.getNodeRole() == NodeRole.PEER) {
            if (getConfig().isBackupActive()) {

                if (!getConfig().availableForBackup()) {
                    // #if LOGGER >= WARN
                    LOGGER.warn("--------------------------------------------------------------------------------------");
                    LOGGER.warn("- This peer replicates its data to other peers but does NOT log data of other peers! -");
                    LOGGER.warn("--------------------------------------------------------------------------------------");
                    // #endif /* LOGGER >= WARN */
                }

                m_event.registerListener(this, NodeFailureEvent.class);
                m_event.registerListener(this, NodeJoinEvent.class);

                m_backupRanges = new ArrayList<>();

                m_backupRangeTree = new BackupRangeTree((short) 10, m_nodeID);

                m_currentBackupRange = null;
                if (getConfig().getForceDisjunctiveFirstBackupPeers()) {
                    m_firstBackupPeers = new ArrayList<>();
                }

                m_lock = new ReentrantReadWriteLock(false);

                m_network.registerMessageType(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_INIT_BACKUP_RANGE_REQUEST, InitBackupRangeRequest.class);
                m_network.registerMessageType(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_INIT_BACKUP_RANGE_RESPONSE,
                        InitBackupRangeResponse.class);
                m_network.registerMessageType(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_INIT_RECOVERED_BACKUP_RANGE_REQUEST,
                        InitRecoveredBackupRangeRequest.class);
                m_network.registerMessageType(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_INIT_RECOVERED_BACKUP_RANGE_RESPONSE,
                        InitRecoveredBackupRangeResponse.class);
            }
        }

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        return true;
    }

    /**
     * Checks if a new backup range must be created and initialized by adding given chunk
     *
     * @param p_dataStructure
     *         the DataStructure
     * @return the BackupRange the Chunk was put in
     * @lock MemoryManager must be write locked
     */
    private BackupRange registerValidChunk(final DataStructure p_dataStructure) {
        return registerValidChunk(p_dataStructure.getID(), p_dataStructure.sizeofObject());
    }

    /**
     * Checks if a new backup range must be created and initialized by adding given chunk
     *
     * @param p_chunkID
     *         the current ChunkID
     * @param p_size
     *         the size of the new created chunk
     * @return the BackupRange the Chunk was put in
     * @lock MemoryManager must be write locked
     */
    private BackupRange registerValidChunk(final long p_chunkID, final int p_size) {
        BackupRange ret = null;
        final int size;

        size = p_size + m_log.getApproxHeaderSize(ChunkID.getCreatorID(p_chunkID), ChunkID.getLocalID(p_chunkID), p_size);

        // First chunk to register -> initialize backup range
        if (m_currentBackupRange == null) {
            initializeNewBackupRange();
            ret = m_currentBackupRange;
        } else {
            if (ChunkID.getCreatorID(p_chunkID) == m_nodeID) {
                // Locally created chunk
                long localID = ChunkID.getLocalID(p_chunkID);
                if (localID > m_currentLocalID) {
                    // New ChunkID -> determine backup range (below)
                    m_currentLocalID = localID;
                } else {
                    // Locally re-used ChunkID -> try backup range of chunk that used the ChunkID before
                    m_lock.writeLock().lock();
                    short rangeID = m_backupRangeTree.getBackupRange(localID);
                    ret = m_backupRanges.get(rangeID);

                    if (ret.fits(size)) {
                        // New chunk fits in backup range of old chunk
                        ret.addChunk(size);
                        m_lock.writeLock().unlock();
                        return ret;
                    } else {
                        // New chunk does not fit -> determine other backup range (below)
                        m_lock.writeLock().unlock();
                    }
                }
            }

            if (!m_currentBackupRange.fits(size)) {
                // Does not fit in current backup range -> check other backup range
                for (BackupRange backupRange : m_backupRanges) {
                    if (backupRange.fits(size)) {
                        ret = backupRange;
                        break;
                    }
                }

                if (ret == null) {
                    // Chunk does not fit in any existing backup range -> create another one
                    initializeNewBackupRange();
                    ret = m_currentBackupRange;
                }
            } else {
                ret = m_currentBackupRange;
            }
        }

        // Put ChunkID and RangeID in backup range tree
        m_lock.writeLock().lock();
        m_backupRangeTree.putChunkID(p_chunkID, ret.getRangeID());
        m_lock.writeLock().unlock();

        ret.addChunk(size);

        return ret;
    }

    /**
     * Initializes a new backup range
     *
     * @lock MemoryManager must be write locked
     */
    private void initializeNewBackupRange() {
        short[] backupPeers;
        BackupRange backupRange;

        backupRange = determineBackupPeers();

        if (backupRange != null) {
            m_currentBackupRange = backupRange;
            m_backupRanges.add(backupRange);
            m_backupRangeTree.initializeNewBackupRange(backupRange.getRangeID());
            m_lookup.initRange(backupRange);
            m_log.initBackupRange(backupRange);

            backupPeers = backupRange.getBackupPeers();
            int counter = 1;
            for (short backupPeer : backupPeers) {
                if (backupPeer != NodeID.INVALID_ID) {
                    // #if LOGGER >= INFO
                    LOGGER.info("%d. backup peer determined for new range %s: %s", counter++, backupRange.getRangeID(), NodeID.toHexString(backupPeer));
                    // #endif /* LOGGER >= INFO */
                }
            }
        } else {
            // #if LOGGER >= ERROR
            LOGGER.info("Backup range could not be determined!");
            // #endif /* LOGGER >= ERROR */
        }
    }

    /**
     * Determines a new backup peer to replace a failed one
     *
     * @param p_currentBackupPeers
     *         all current backup peers
     * @return the replacement
     * @lock m_lock must be write-locked
     */

    private short determineReplacementBackupPeer(final short[] p_currentBackupPeers) {
        short ret;
        short currentPeer;
        short numberOfPeers;

        List<Short> peers;
        // Get all other online backup peers
        peers = m_boot.getIDsOfAvailableBackupPeers();
        numberOfPeers = (short) peers.size();

        if (numberOfPeers < getConfig().getReplicationFactor()) {
            // #if LOGGER >= WARN
            LOGGER.warn("Less than three peers for backup available. Replication will be incomplete!");
            // #endif /* LOGGER >= WARN */

            return NodeID.INVALID_ID;
        }

        if (numberOfPeers < getConfig().getReplicationFactor() * 2) {
            // #if LOGGER >= WARN
            LOGGER.warn("Less than six peers for backup available. Some peers may store more" + " than one backup range of a node!");
            // #endif /* LOGGER >= WARN */
        }

        // Determine backup peer
        while (true) {
            // Choose random peer
            currentPeer = peers.get((int) (Math.random() * numberOfPeers));

            // Check if peer is a backup peer already
            for (int i = 0; i < p_currentBackupPeers.length; i++) {
                if (currentPeer == p_currentBackupPeers[i]) {
                    currentPeer = NodeID.INVALID_ID;
                    break;
                }
            }

            if (currentPeer != NodeID.INVALID_ID) {
                ret = currentPeer;
                break;
            }
        }

        return ret;
    }

    /**
     * Determines backup peers
     *
     * @return the backup peers
     */
    private BackupRange determineBackupPeers() {
        BackupRange ret;
        boolean ready = false;
        boolean insufficientPeers = false;
        short index = 0;
        short[] oldBackupPeers = null;
        short[] newBackupPeers;
        short numberOfPeers;

        List<Short> peers;
        // Get all other online backup peers
        peers = m_boot.getIDsOfAvailableBackupPeers();
        numberOfPeers = (short) peers.size();

        m_lock.writeLock().lock();
        if (numberOfPeers < getConfig().getReplicationFactor()) {
            // #if LOGGER >= WARN
            LOGGER.warn("Less than three peers for backup available. Replication will be incomplete!");
            // #endif /* LOGGER >= WARN */

            newBackupPeers = new short[getConfig().getReplicationFactor()];
            Arrays.fill(newBackupPeers, NodeID.INVALID_ID);

            insufficientPeers = true;
        } else if (numberOfPeers < getConfig().getReplicationFactor() * 2) {
            // #if LOGGER >= WARN
            LOGGER.warn("Less than six peers for backup available. Some peers may store more" + " than one backup range of a node!");
            // #endif /* LOGGER >= WARN */

            oldBackupPeers = new short[getConfig().getReplicationFactor()];
            Arrays.fill(oldBackupPeers, NodeID.INVALID_ID);

            newBackupPeers = new short[getConfig().getReplicationFactor()];
            Arrays.fill(newBackupPeers, NodeID.INVALID_ID);
        } else {
            if (m_currentBackupRange != null) {
                oldBackupPeers = new short[getConfig().getReplicationFactor()];
                System.arraycopy(m_currentBackupRange.getBackupPeers(), 0, oldBackupPeers, 0, getConfig().getReplicationFactor());
            } else {
                oldBackupPeers = new short[getConfig().getReplicationFactor()];
                Arrays.fill(oldBackupPeers, NodeID.INVALID_ID);
            }

            newBackupPeers = new short[getConfig().getReplicationFactor()];
            Arrays.fill(newBackupPeers, NodeID.INVALID_ID);
        }

        if (insufficientPeers) {
            if (numberOfPeers > 0) {
                // Determine backup peers
                for (int i = 0; i < numberOfPeers; i++) {
                    while (!ready) {
                        index = (short) (Math.random() * numberOfPeers);
                        ready = true;
                        for (int j = 0; j < i; j++) {
                            if (peers.get(index) == newBackupPeers[j]) {
                                ready = false;
                                break;
                            }
                        }
                    }
                    newBackupPeers[i] = peers.get(index);
                    ready = false;
                }

                if (getConfig().getForceDisjunctiveFirstBackupPeers()) {
                    m_firstBackupPeers.add(newBackupPeers[0]);
                }
            }
        } else {
            if (!getConfig().getForceDisjunctiveFirstBackupPeers()) {
                // Determine backup peers
                for (int i = 0; i < getConfig().getReplicationFactor(); i++) {
                    while (!ready) {
                        index = (short) (Math.random() * numberOfPeers);
                        ready = true;
                        for (int j = 0; j < i; j++) {
                            if (peers.get(index) == oldBackupPeers[j] || peers.get(index) == newBackupPeers[j]) {
                                ready = false;
                                break;
                            }
                        }
                    }
                    newBackupPeers[i] = peers.get(index);
                    ready = false;
                }
            } else {
                List<Short> availablePeers;

                // Use a disjunctive first backup peers if possible
                if (numberOfPeers > m_firstBackupPeers.size()) {
                    // Remove already used first backup peers
                    availablePeers = new ArrayList<>();
                    availablePeers.addAll(peers);
                    availablePeers.removeAll(m_firstBackupPeers);
                } else {
                    // Not enough backup peers available to be picky
                    // #if LOGGER >= WARN
                    LOGGER.warn("Too less peers available for disjunctive backup strategy. Backup determination starts all over again!");
                    // #endif /* LOGGER >= WARN */

                    m_firstBackupPeers.clear();
                    availablePeers = peers;
                }

                // Determine backup peers
                for (int i = 0; i < getConfig().getReplicationFactor(); i++) {
                    while (!ready) {
                        index = (short) (Math.random() * availablePeers.size());
                        ready = true;
                        for (int j = 0; j < i; j++) {
                            if (availablePeers.get(index) == oldBackupPeers[j] || availablePeers.get(index) == newBackupPeers[j]) {
                                ready = false;
                                break;
                            }
                        }
                    }
                    newBackupPeers[i] = availablePeers.get(index);
                    ready = false;

                    if (i == 0) {
                        availablePeers = peers;
                        m_firstBackupPeers.add(newBackupPeers[i]);
                    }
                }
            }
        }

        ret = new BackupRange((short) m_backupRanges.size(), newBackupPeers);
        m_lock.writeLock().unlock();

        return ret;
    }
}
