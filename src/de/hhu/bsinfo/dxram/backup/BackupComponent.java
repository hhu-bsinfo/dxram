/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkBackupComponent;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.event.EventListener;
import de.hhu.bsinfo.dxram.failure.events.NodeFailureEvent;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.messages.InitRequest;
import de.hhu.bsinfo.dxram.log.messages.InitResponse;
import de.hhu.bsinfo.dxram.log.messages.LogMessages;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.NodeID;
import de.hhu.bsinfo.utils.unit.StorageUnit;

/**
 * Component for managing backup ranges.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 30.03.2016
 */
public class BackupComponent extends AbstractDXRAMComponent implements EventListener<NodeFailureEvent> {

    private static final Logger LOGGER = LogManager.getFormatterLogger(BackupComponent.class.getSimpleName());

    // configuration values
    @Expose
    private boolean m_backupActive = false;
    @Expose
    private String m_backupDirectory = "./log/";
    @Expose
    private StorageUnit m_backupRangeSize = new StorageUnit(256, StorageUnit.MB);
    // The replication factor must must be in [1, 4], for disabling replication set m_backupActive to false
    @Expose
    private byte m_replicationFactor = 3;

    // dependent components
    private AbstractBootComponent m_boot;
    private ChunkBackupComponent m_chunk;
    private LookupComponent m_lookup;
    private LogComponent m_log;
    private EventComponent m_event;
    private NetworkComponent m_network;

    // private state
    private long m_rangeSize;
    private boolean m_firstRangeInitialized;

    private short m_nodeID;
    private long m_lastChunkID;

    private BackupRange m_currentBackupRange;
    private ArrayList<BackupRange> m_ownBackupRanges;

    private BackupRange m_currentMigrationBackupRange;
    private ArrayList<BackupRange> m_migrationBackupRanges;
    // ChunkID -> migration backup range
    private MigrationBackupTree m_migrationsTree;

    private ReentrantReadWriteLock m_lock;

    /**
     * Creates the backup component
     */
    public BackupComponent() {
        super(DXRAMComponentOrder.Init.BACKUP, DXRAMComponentOrder.Shutdown.BACKUP);
    }

    /**
     * Returns whether backup is enabled or not
     *
     * @return whether backup is enabled or not
     */
    public boolean isActive() {
        return m_backupActive;
    }

    /**
     * Return the path to all logs
     *
     * @return the backup directory
     */
    public String getBackupDirectory() {
        return m_backupDirectory;
    }

    /**
     * Returns the backup peers for current migration backup range
     *
     * @return the backup peers for current migration backup range
     */
    public short[] getCopyOfCurrentMigrationBackupPeers() {
        short[] ret;

        m_lock.readLock().lock();
        ret = m_currentMigrationBackupRange.getCopyOfBackupPeers();
        m_lock.readLock().unlock();

        return ret;
    }

    /**
     * Registers peer in superpeer overlay
     */
    public void registerPeer() {
        short[] backupPeers = new short[m_replicationFactor];
        Arrays.fill(backupPeers, (short) -1);

        m_lookup.initRange(0, m_nodeID, backupPeers);
    }

    /**
     * Initializes the backup range for current locations
     * and determines new backup peers if necessary
     *
     * @param p_chunkID
     *     the current ChunkID
     * @param p_size
     *     the size of the new created chunk
     * @note must be serialized by MemoryManager
     */
    public void initBackupRange(final long p_chunkID, final int p_size) {
        final int size;
        final long localID = ChunkID.getLocalID(p_chunkID);
        BackupRange backupRange;

        m_lastChunkID = p_chunkID;
        if (m_backupActive) {
            size = p_size + m_log.getAproxHeaderSize(m_nodeID, localID, p_size);
            if (!m_firstRangeInitialized && localID == 1) {
                // First Chunk has LocalID 1, but there is a Chunk with LocalID 0 for hosting the name service
                // This is the first put and p_localID is not reused
                backupRange = determineBackupPeers(0);

                m_lookup.initRange((long) m_nodeID << 48, m_nodeID, backupRange.getBackupPeers());
                m_log.initBackupRange((long) m_nodeID << 48, backupRange.getBackupPeers());
                m_rangeSize = size;
                m_firstRangeInitialized = true;
            } else if (m_rangeSize + size > m_backupRangeSize.getBytes()) {
                backupRange = determineBackupPeers(localID);

                m_lookup.initRange(((long) m_nodeID << 48) + localID, m_nodeID, backupRange.getBackupPeers());
                m_log.initBackupRange(((long) m_nodeID << 48) + localID, backupRange.getBackupPeers());
                m_rangeSize = size;
            } else {
                m_rangeSize += size;
            }
        } else if (!m_firstRangeInitialized) {
            short[] backupPeers = new short[m_replicationFactor];
            Arrays.fill(backupPeers, (short) -1);

            m_lookup.initRange(((long) m_nodeID << 48) + 0xFFFFFFFFFFFFL, m_nodeID, backupPeers);
            m_firstRangeInitialized = true;
        }
    }

    /**
     * Returns the corresponding backup peers
     *
     * @param p_chunkID
     *     the ChunkID
     * @return the backup peers
     */
    public short[] getCopyOfBackupPeersForLocalChunks(final long p_chunkID) {
        short[] ret = null;

        m_lock.readLock().lock();
        if (ChunkID.getCreatorID(p_chunkID) == m_nodeID) {
            for (int i = m_ownBackupRanges.size() - 1; i >= 0; i--) {
                if (m_ownBackupRanges.get(i).getRangeID() <= ChunkID.getLocalID(p_chunkID)) {
                    ret = m_ownBackupRanges.get(i).getCopyOfBackupPeers();
                    break;
                }
            }
        } else {
            ret = m_migrationBackupRanges.get(m_migrationsTree.getBackupRange(p_chunkID)).getCopyOfBackupPeers();
        }
        m_lock.readLock().unlock();

        return ret;
    }

    /**
     * Returns the corresponding backup peers
     *
     * @param p_chunkID
     *     the ChunkID
     * @return the backup peers
     */
    public long getBackupPeersForLocalChunks(final long p_chunkID) {
        long ret = -1;

        m_lock.readLock().lock();
        if (ChunkID.getCreatorID(p_chunkID) == m_nodeID) {
            for (int i = m_ownBackupRanges.size() - 1; i >= 0; i--) {
                if (m_ownBackupRanges.get(i).getRangeID() <= ChunkID.getLocalID(p_chunkID)) {
                    ret = m_ownBackupRanges.get(i).getBackupPeersAsLong();
                    break;
                }
            }
        } else {
            ret = m_migrationBackupRanges.get(m_migrationsTree.getBackupRange(p_chunkID)).getBackupPeersAsLong();
        }
        m_lock.readLock().unlock();

        return ret;
    }

    /**
     * Initializes a new migration backup range
     */
    public void initNewMigrationBackupRange() {
        BackupRange backupRange;

        m_lock.writeLock().lock();
        backupRange = determineBackupPeers(-1);
        m_migrationsTree.initNewBackupRange();
        m_lock.writeLock().unlock();

        m_lookup.initRange(((long) -1 << 48) + backupRange.getRangeID(), m_nodeID, backupRange.getBackupPeers());
        m_log.initBackupRange(((long) -1 << 48) + backupRange.getRangeID(), backupRange.getBackupPeers());
    }

    /**
     * Puts a migrated chunk into the migration tree
     *
     * @param p_chunk
     *     the migrated chunk
     * @return the RangeID of the migration backup range the chunk was put in
     */
    public byte addMigratedChunk(final Chunk p_chunk) {
        final byte rangeID = (byte) m_currentMigrationBackupRange.getRangeID();

        m_lock.writeLock().lock();
        m_migrationsTree.putObject(p_chunk.getID(), rangeID, p_chunk.getDataSize());
        m_lock.writeLock().unlock();

        return rangeID;
    }

    /**
     * Checks if given log entry fits in current migration backup range
     *
     * @param p_size
     *     the range size
     * @param p_logEntrySize
     *     the log entry size
     * @return whether the entry and range fits in backup range
     */
    public boolean fitsInCurrentMigrationBackupRange(final long p_size, final int p_logEntrySize) {
        boolean ret;

        m_lock.readLock().lock();
        ret = m_migrationsTree.fits(p_size + p_logEntrySize) && (m_migrationsTree.size() != 0 || p_size > 0);
        m_lock.readLock().unlock();

        return ret;
    }

    @Override
    public void eventTriggered(final NodeFailureEvent p_event) {
        short failedPeer;
        short currentBackupPeer;
        short newBackupPeer;
        long firstChunkID;
        long lastChunkID;
        long rangeID;
        BackupRange currentBackupRange;
        short[] backupPeers;

        if (p_event.getRole() == NodeRole.PEER) {
            failedPeer = p_event.getNodeID();

            // Replace failed peer in all own backup ranges
            for (int i = 0; i < m_ownBackupRanges.size(); i++) {
                m_lock.writeLock().lock();
                currentBackupRange = m_ownBackupRanges.get(i);
                backupPeers = currentBackupRange.getCopyOfBackupPeers();
                firstChunkID = currentBackupRange.getRangeID();

                for (int j = 0; j < backupPeers.length; j++) {
                    currentBackupPeer = backupPeers[j];
                    if (currentBackupPeer == failedPeer) {
                        // Determine new backup peer and replace it in backup range
                        newBackupPeer = determineReplacementBackupPeer(backupPeers, failedPeer, firstChunkID);
                        currentBackupRange.replaceBackupPeer(j, newBackupPeer);
                        m_lock.writeLock().unlock();

                        // Send new backup peer all chunks of backup range
                        if (newBackupPeer != -1) {
                            if (m_ownBackupRanges.size() > i + 1) {
                                // There is a succeeding backup range
                                lastChunkID = m_ownBackupRanges.get(i + 1).getRangeID() - 1;
                            } else {
                                // This is the current backup range
                                lastChunkID = m_lastChunkID;
                            }
                            m_chunk.replicateBackupRange(newBackupPeer, firstChunkID, lastChunkID);
                        }

                        // Inform responsible superpeer to update backup range
                        m_lookup.replaceBackupPeer(firstChunkID, failedPeer, newBackupPeer);

                        break;
                    }
                    if (j == backupPeers.length - 1) {
                        // Failed peer was not responsible for backup range
                        m_lock.writeLock().unlock();
                    }
                }
            }

            // Replace failed peer in all migration backup ranges
            for (int i = 0; i < m_migrationBackupRanges.size(); i++) {
                m_lock.writeLock().lock();
                currentBackupRange = m_migrationBackupRanges.get(i);
                backupPeers = currentBackupRange.getCopyOfBackupPeers();
                rangeID = currentBackupRange.getRangeID();

                for (int j = 0; j < backupPeers.length; j++) {
                    currentBackupPeer = backupPeers[j];
                    if (currentBackupPeer == failedPeer) {
                        // Determine new backup peer and replace it in backup range
                        newBackupPeer = determineReplacementBackupPeer(backupPeers, failedPeer, rangeID);
                        currentBackupRange.replaceBackupPeer(j, newBackupPeer);
                        m_lock.writeLock().unlock();

                        // Send new backup peer all chunks of backup range
                        if (newBackupPeer != -1) {
                            long[] chunkIDs = m_migrationsTree.getAllChunkIDsOfRange((byte) rangeID);
                            m_chunk.replicateBackupRange(newBackupPeer, chunkIDs, (byte) rangeID);
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
        }
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_chunk = p_componentAccessor.getComponent(ChunkBackupComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
        m_log = p_componentAccessor.getComponent(LogComponent.class);
        m_event = p_componentAccessor.getComponent(EventComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        if (m_replicationFactor < 1 || m_replicationFactor > 4) {
            // #if LOGGER >= ERROR
            LOGGER.warn("Replication factor must be in [1, 4]!");
            // #endif /* LOGGER >= ERROR */

            return false;
        }
        BackupRange.setReplicationFactor(m_replicationFactor);

        m_event.registerListener(this, NodeFailureEvent.class);
        m_nodeID = m_boot.getNodeID();
        if (m_backupActive && m_boot.getNodeRole() == NodeRole.PEER) {
            m_ownBackupRanges = new ArrayList<>();
            m_migrationBackupRanges = new ArrayList<>();
            m_migrationsTree = new MigrationBackupTree((short) 10, m_backupRangeSize.getBytes());
            m_currentBackupRange = null;
            m_currentMigrationBackupRange = new BackupRange(-1, null);
            m_rangeSize = 0;

            m_lock = new ReentrantReadWriteLock(false);

            m_network.registerMessageType(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_INIT_REQUEST, InitRequest.class);
            m_network.registerMessageType(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_INIT_RESPONSE, InitResponse.class);
        }
        m_firstRangeInitialized = false;

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        return true;
    }

    /**
     * Determines a new backup peer to replace a failed one
     *
     * @param p_currentBackupPeers
     *     all current backup peers
     * @param p_failedPeer
     *     the NodeID of the failed backup peer
     * @param p_firstChunkIDOrRangeID
     *     the first ChunkID of a backup range or the RangeID for a migration backup range
     * @return the replacement
     * @lock m_lock must be write-locked
     */
    private short determineReplacementBackupPeer(final short[] p_currentBackupPeers, final short p_failedPeer, final long p_firstChunkIDOrRangeID) {
        short ret;
        short currentPeer;
        short numberOfPeers;

        List<Short> peers;
        // Get all other online peers
        peers = m_boot.getIDsOfOnlinePeers();
        numberOfPeers = (short) peers.size();

        if (numberOfPeers < m_replicationFactor) {
            // #if LOGGER >= WARN
            LOGGER.warn("Less than three peers for backup available. Replication will be incomplete!");
            // #endif /* LOGGER >= WARN */

            return -1;
        }

        if (numberOfPeers < m_replicationFactor * 2) {
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
                    currentPeer = -1;
                    break;
                }
            }

            if (currentPeer != -1) {
                // #if LOGGER >= INFO
                LOGGER.info("Determined new backup peer for range 0x%X to replace 0x%X: 0x%X", ((long) m_nodeID << 48) + p_firstChunkIDOrRangeID, p_failedPeer,
                    currentPeer);
                // #endif /* LOGGER >= INFO */

                ret = currentPeer;
                break;
            }
        }

        return ret;
    }

    /**
     * Determines backup peers
     *
     * @param p_localID
     *     the current LocalID
     * @return the backup peers
     */
    private BackupRange determineBackupPeers(final long p_localID) {
        BackupRange ret = null;
        boolean ready = false;
        boolean insufficientPeers = false;
        short index = 0;
        short[] oldBackupPeers = null;
        short[] newBackupPeers;
        short numberOfPeers;

        List<Short> peers;
        // Get all other online peers
        peers = m_boot.getIDsOfOnlinePeers();
        numberOfPeers = (short) peers.size();

        m_lock.writeLock().lock();
        if (numberOfPeers < m_replicationFactor) {
            // #if LOGGER >= WARN
            LOGGER.warn("Less than three peers for backup available. Replication will be incomplete!");
            // #endif /* LOGGER >= WARN */

            newBackupPeers = new short[m_replicationFactor];
            Arrays.fill(newBackupPeers, (short) -1);

            insufficientPeers = true;
        } else if (numberOfPeers < m_replicationFactor * 2) {
            // #if LOGGER >= WARN
            LOGGER.warn("Less than six peers for backup available. Some peers may store more" + " than one backup range of a node!");
            // #endif /* LOGGER >= WARN */

            oldBackupPeers = new short[m_replicationFactor];
            Arrays.fill(oldBackupPeers, (short) -1);

            newBackupPeers = new short[m_replicationFactor];
            Arrays.fill(newBackupPeers, (short) -1);
        } else {
            if (m_currentBackupRange != null) {
                oldBackupPeers = new short[m_replicationFactor];
                for (int i = 0; i < m_replicationFactor; i++) {
                    if (p_localID > -1) {
                        oldBackupPeers[i] = m_currentBackupRange.getBackupPeers()[i];
                    } else {
                        oldBackupPeers[i] = m_currentMigrationBackupRange.getBackupPeers()[i];
                    }
                }
            } else {
                oldBackupPeers = new short[m_replicationFactor];
                Arrays.fill(oldBackupPeers, (short) -1);
            }

            newBackupPeers = new short[m_replicationFactor];
            Arrays.fill(newBackupPeers, (short) -1);
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
                    // #if LOGGER >= INFO
                    LOGGER.info("%d. backup peer determined for new range %s: %s", i + 1, ChunkID.toHexString(((long) m_nodeID << 48) + p_localID),
                        NodeID.toHexString(peers.get(index)));
                    // #endif /* LOGGER >= INFO */
                    newBackupPeers[i] = peers.get(index);
                    ready = false;
                }
                if (p_localID > -1) {
                    m_currentBackupRange = new BackupRange(p_localID, newBackupPeers);
                } else {
                    m_currentMigrationBackupRange = new BackupRange(m_currentMigrationBackupRange.getRangeID() + 1, newBackupPeers);
                }
            } else {
                if (p_localID > -1) {
                    m_currentBackupRange = new BackupRange(p_localID, null);
                } else {
                    m_currentMigrationBackupRange = new BackupRange(m_currentMigrationBackupRange.getRangeID() + 1, null);
                }
            }
        } else {
            // Determine backup peers
            for (int i = 0; i < m_replicationFactor; i++) {
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
                // #if LOGGER >= INFO
                LOGGER.info("%d. backup peer determined for new range %s: %s", i + 1, ChunkID.toHexString(((long) m_nodeID << 48) + p_localID),
                    NodeID.toHexString(peers.get(index)));
                // #endif /* LOGGER >= INFO */
                newBackupPeers[i] = peers.get(index);
                ready = false;
            }
            if (p_localID > -1) {
                m_currentBackupRange = new BackupRange(p_localID, newBackupPeers);
            } else {
                m_currentMigrationBackupRange = new BackupRange(m_currentMigrationBackupRange.getRangeID() + 1, newBackupPeers);
            }
        }

        if (numberOfPeers > 0) {
            if (p_localID > -1) {
                m_ownBackupRanges.add(m_currentBackupRange);
                // Return a copy of the new backup range to avoid race conditions
                ret = new BackupRange(m_currentBackupRange.getRangeID(), m_currentBackupRange.getCopyOfBackupPeers());
            } else {
                m_migrationBackupRanges.add(m_currentMigrationBackupRange);
                // Return a copy of the new backup range to avoid race conditions
                ret = new BackupRange(m_currentMigrationBackupRange.getRangeID(), m_currentMigrationBackupRange.getCopyOfBackupPeers());
            }
        }
        m_lock.writeLock().unlock();

        return ret;
    }
}
