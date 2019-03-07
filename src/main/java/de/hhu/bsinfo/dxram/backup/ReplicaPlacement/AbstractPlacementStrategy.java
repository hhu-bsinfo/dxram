package de.hhu.bsinfo.dxram.backup.ReplicaPlacement;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.backup.BackupRange;

/**
 * Interface for replica placement strategies.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 01.12.2017
 */
public abstract class AbstractPlacementStrategy {
    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractPlacementStrategy.class);

    int m_replicationFactor;

    boolean m_disjunctive;
    ArrayList<BackupPeer> m_usedBackupPeers;

    boolean m_rackAware;
    boolean m_switchAware;

    AbstractPlacementStrategy(final int p_replicationFactor, final boolean p_disjunctiveFirstBackupPeer,
            final boolean p_rackAware, final boolean p_switchAware) {
        m_replicationFactor = p_replicationFactor;

        m_disjunctive = p_disjunctiveFirstBackupPeer;
        if (p_disjunctiveFirstBackupPeer) {
            m_usedBackupPeers = new ArrayList<>();
            m_disjunctive = true;
        }
        m_rackAware = p_rackAware;
        m_switchAware = p_switchAware;
    }

    /**
     * Initializes backup placement strategy with initial set of peers
     *
     * @return whether the initialization was successful or not
     */
    public abstract boolean initialize(List<BackupPeer> p_availablePeers);

    /**
     * Determines backup peers
     *
     * @return the backup peers
     * @lock BackupComponent.m_lock must be write-locked
     */
    public abstract BackupRange determineBackupPeers(short p_backupRangeID, List<BackupPeer> p_availablePeers,
            BackupRange p_currentBackupRange);

    /**
     * Determines a new backup peer to replace a failed one
     *
     * @param p_currentBackupPeers
     *         all current backup peers
     * @return the replacement
     * @lock BackupComponent.m_lock must be write-locked
     */
    public abstract BackupPeer determineReplacementBackupPeer(BackupPeer[] p_currentBackupPeers,
            List<BackupPeer> p_availablePeers);

    /**
     * Adds a new backup peer
     *
     * @param p_newPeer
     *         new available backup peer
     * @lock BackupComponent.m_lock must be write-locked
     */
    public abstract void addNewBackupPeer(BackupPeer p_newPeer);

    /**
     * Returns the replication factor
     *
     * @return the replication factor
     */
    public int getReplicationFactor() {
        return m_replicationFactor;
    }

    /**
     * Whether it is disjunctive
     *
     * @return true, if first backup peer is chosen disjunctive
     */
    public boolean isDisjunctive() {
        return m_disjunctive;
    }

    /**
     * Whether it is rack-aware
     *
     * @return true, if rack-aware
     */
    public boolean isRackAware() {
        return m_rackAware;
    }

    /**
     * Whether it is switch-aware
     *
     * @return true, if switch-aware
     */
    public boolean isSwitchAware() {
        return m_switchAware;
    }

    /**
     * Add a new backup peer by random select. It is guaranteed that the new backup peer is not a duplicate.
     *
     * @param p_availablePeers
     *         all available peers to select from
     * @param p_backupPeers
     *         array to put the new backup peer in
     * @param p_numberOfNodes
     *         the number of nodes in the list to consider (list might be larger)
     * @param p_currentIndex
     *         the array index
     */
    void addNewRandomBackupPeer(final List<BackupPeer> p_availablePeers, final BackupPeer[] p_backupPeers,
            final int p_numberOfNodes, final int p_currentIndex,
            final Random p_rand) {
        BackupPeer newBackupPeer = null;
        BackupPeer currentBackupPeer;
        boolean ready = false;
        while (!ready) {
            newBackupPeer = p_availablePeers.get(p_rand.nextInt(p_numberOfNodes));
            ready = true;
            for (int j = 0; j < p_currentIndex; j++) {
                currentBackupPeer = p_backupPeers[j];
                if (currentBackupPeer == null) {
                    break;
                }

                if (newBackupPeer.getNodeID() == currentBackupPeer.getNodeID() ||
                        m_rackAware && newBackupPeer.getRack() == currentBackupPeer.getRack() ||
                        m_switchAware && newBackupPeer.getSwitch() == currentBackupPeer.getSwitch()) {
                    ready = false;
                    break;
                }
            }
        }
        p_backupPeers[p_currentIndex] = newBackupPeer;
    }

    /**
     * Add a new backup peer by random select. It is guaranteed that the new backup peer is not a duplicate
     * (not in p_oldBackupPeers and p_newBackupPeers).
     *
     * @param p_availablePeers
     *         all available peers to select from
     * @param p_oldBackupPeers
     *         array to compare with
     * @param p_newBackupPeers
     *         array to put the new backup peer in
     * @param p_numberOfNodes
     *         the number of nodes in the list to consider (list might be larger)
     * @param p_currentIndex
     *         the array index
     */
    void addNewRandomBackupPeer(final List<BackupPeer> p_availablePeers, final BackupPeer[] p_oldBackupPeers,
            final BackupPeer[] p_newBackupPeers,
            final int p_numberOfNodes, final int p_currentIndex, final Random p_rand) {
        BackupPeer newBackupPeer = null;
        BackupPeer currentBackupPeer;
        boolean ready = false;
        while (!ready) {
            newBackupPeer = p_availablePeers.get(p_rand.nextInt(p_numberOfNodes));
            ready = true;
            for (int j = 0; j < p_currentIndex; j++) {
                currentBackupPeer = p_newBackupPeers[j];
                if (newBackupPeer.getNodeID() == currentBackupPeer.getNodeID() ||
                        p_oldBackupPeers[j] != null && newBackupPeer.getNodeID() == p_oldBackupPeers[j].getNodeID() ||
                        m_rackAware && newBackupPeer.getRack() == currentBackupPeer.getRack() ||
                        m_switchAware && newBackupPeer.getSwitch() == currentBackupPeer.getSwitch()) {
                    ready = false;
                    break;
                }
            }
        }
        p_newBackupPeers[p_currentIndex] = newBackupPeer;
    }

    /**
     * Check if awareness strategies are applicable for initial set of peers
     *
     * @param p_availablePeers
     *         all initially available backup peers
     */
    void checkAwarenessApplicability(final List<BackupPeer> p_availablePeers) {
        if (m_rackAware || m_switchAware) {
            boolean isNew;
            ArrayList<Short> racks = new ArrayList<>();
            ArrayList<Short> switches = new ArrayList<>();

            for (BackupPeer peer : p_availablePeers) {
                isNew = true;
                for (Short currentRack : racks) {
                    if (currentRack == peer.getRack()) {
                        isNew = false;
                        break;
                    }
                }
                if (isNew) {
                    racks.add(peer.getRack());
                }

                isNew = true;
                for (Short currentSwitch : switches) {
                    if (currentSwitch == peer.getSwitch()) {
                        isNew = false;
                        break;
                    }
                }
                if (isNew) {
                    switches.add(peer.getSwitch());
                }
            }

            if (m_rackAware && racks.size() < m_replicationFactor) {
                LOGGER.warn("Rack-awareness not applicable with %d racks in initial set of peers!", racks.size());

                m_rackAware = false;
            } else if (m_rackAware && racks.size() <= 2 * m_replicationFactor) {
                LOGGER.warn("Rack-awareness restricts replica placement with only %d racks in initial set of peers!",
                        racks.size());

            }

            if (m_switchAware && switches.size() < m_replicationFactor) {
                LOGGER.warn("Switch-awareness not applicable with %d switches in initial set of peers!",
                        switches.size());

                m_switchAware = false;
            } else if (m_switchAware && switches.size() <= 2 * m_replicationFactor) {
                LOGGER.warn(
                        "Switch-awareness restricts replica placement with only %d switches in initial set of peers!",
                        switches.size());

            }
        }
    }
}
