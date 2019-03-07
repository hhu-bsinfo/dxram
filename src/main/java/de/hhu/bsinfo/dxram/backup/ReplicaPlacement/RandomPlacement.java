package de.hhu.bsinfo.dxram.backup.ReplicaPlacement;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.backup.BackupRange;

/**
 * Random replica placement strategy.
 * Every backup peer is chosen randomly without duplicates within one backup range.
 * If possible, all backup peers from previous backup range are spared for the next backup range.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 01.12.2017
 */
public class RandomPlacement extends AbstractPlacementStrategy {
    private static final Logger LOGGER = LogManager.getFormatterLogger(RandomPlacement.class);

    private Random m_rand;

    /**
     * Creates an instance of RandomPlacement
     */
    public RandomPlacement(final int p_replicationFactor, final boolean p_disjunctiveFirstBackupPeer,
            final boolean p_rackAware, final boolean p_switchAware) {
        super(p_replicationFactor, p_disjunctiveFirstBackupPeer, p_rackAware, p_switchAware);
        m_rand = new Random(System.nanoTime());
    }

    @Override
    public boolean initialize(List<BackupPeer> p_availablePeers) {
        checkAwarenessApplicability(p_availablePeers);
        return true;
    }

    @Override
    public void addNewBackupPeer(BackupPeer p_newPeer) {
        // Nothing to do here
    }

    @Override
    public BackupPeer determineReplacementBackupPeer(final BackupPeer[] p_currentBackupPeers,
            final List<BackupPeer> p_availablePeers) {
        BackupPeer ret;
        BackupPeer currentPeer;
        short numberOfPeers;

        numberOfPeers = (short) p_availablePeers.size();

        if (numberOfPeers < m_replicationFactor) {

            LOGGER.warn("Less than three peers for backup available. Replication will be incomplete!");

            return null;
        }

        if (numberOfPeers < m_replicationFactor * 2) {

            LOGGER.warn("Less than six peers for backup available. Some peers may store more" +
                    " than one backup range of a node!");

        }

        // Determine backup peer
        while (true) {
            // Choose random peer
            currentPeer = p_availablePeers.get((int) (Math.random() * numberOfPeers));

            // Check if peer is a backup peer already
            for (int i = 0; i < p_currentBackupPeers.length; i++) {
                if (p_currentBackupPeers[i] != null && currentPeer.getNodeID() == p_currentBackupPeers[i].getNodeID()) {
                    currentPeer = null;
                    break;
                }
            }

            if (currentPeer != null) {
                ret = currentPeer;
                break;
            }
        }

        return ret;
    }

    @Override
    public BackupRange determineBackupPeers(final short p_backupRangeID, final List<BackupPeer> p_availablePeers,
            final BackupRange p_currentBackupRange) {
        BackupRange ret;
        boolean insufficientPeers = false;
        BackupPeer[] oldBackupPeers = null;
        BackupPeer[] newBackupPeers;
        short numberOfPeers;

        numberOfPeers = (short) p_availablePeers.size();

        if (numberOfPeers < m_replicationFactor) {

            LOGGER.warn("Less than %d peers for backup available. Replication will be incomplete!",
                    m_replicationFactor);

            newBackupPeers = new BackupPeer[m_replicationFactor];
            insufficientPeers = true;
        } else if (numberOfPeers < m_replicationFactor * 2) {

            LOGGER.warn("Less than %d peers for backup available. Some peers may store more than one backup " +
                    "range of a node!", 2 * m_replicationFactor);

            oldBackupPeers = new BackupPeer[m_replicationFactor];
            newBackupPeers = new BackupPeer[m_replicationFactor];
        } else {
            if (p_currentBackupRange != null) {
                oldBackupPeers = new BackupPeer[m_replicationFactor];
                System.arraycopy(p_currentBackupRange.getBackupPeers(), 0, oldBackupPeers, 0, m_replicationFactor);
            } else {
                oldBackupPeers = new BackupPeer[m_replicationFactor];
            }

            newBackupPeers = new BackupPeer[m_replicationFactor];
        }

        if (insufficientPeers) {
            if (numberOfPeers > 0) {
                // Determine backup peers
                for (int i = 0; i < numberOfPeers; i++) {
                    addNewRandomBackupPeer(p_availablePeers, newBackupPeers, numberOfPeers, i, m_rand);
                }
            }
        } else {
            BackupPeer firstBackupPeer;
            if (m_disjunctive) {
                // Determine first backup peer disjunctive
                List<BackupPeer> availablePeers;
                if (m_usedBackupPeers.size() < numberOfPeers) {
                    availablePeers = new ArrayList<BackupPeer>();
                    availablePeers.addAll(p_availablePeers);
                    availablePeers.removeAll(m_usedBackupPeers);
                } else {
                    // Not enough backup peers available to be picky

                    LOGGER.warn("Insufficient peers available for disjunctive backup strategy. Backup peers might be " +
                            "used more than once as a first backup peer!");

                    m_usedBackupPeers.clear();
                    availablePeers = p_availablePeers;
                }

                firstBackupPeer = availablePeers.get(m_rand.nextInt(availablePeers.size()));
                m_usedBackupPeers.add(firstBackupPeer);
            } else {
                // Determine first backup peer randomly
                firstBackupPeer = p_availablePeers.get(m_rand.nextInt(numberOfPeers));
            }
            newBackupPeers[0] = firstBackupPeer;

            // Determine other backup peers
            for (int i = 1; i < m_replicationFactor; i++) {
                addNewRandomBackupPeer(p_availablePeers, oldBackupPeers, newBackupPeers, numberOfPeers, i, m_rand);
            }
        }

        ret = new BackupRange(p_backupRangeID, newBackupPeers);

        return ret;
    }
}
