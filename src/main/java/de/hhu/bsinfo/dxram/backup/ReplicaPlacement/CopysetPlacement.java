package de.hhu.bsinfo.dxram.backup.ReplicaPlacement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.backup.BackupRange;

/**
 * Copyset replica placement strategy.
 * Based on the RAMCloud implementation from the paper "Copysets: Reducing the Frequency of Data Loss in Cloud Storage",
 * Asaf Cidon from Stanford University.
 * https://web.stanford.edu/~skatti/pubs/usenix13-copysets.pdf
 * Differences:
 * - DXRAM determines backup peers not for single chunks but for backup ranges containing many chunks. Therefore, the
 * maximal number of copysets is smaller. Still, with copyset replication the probability for data loss can be reduced.
 * - We do not determine the copysets on one server and use them globally. Instead, every server determines the
 * copysets on its own. Given the set of available nodes is identical, every node determines the same copysets.
 * But, copysets can differ when nodes are added because nodes might detect the joining nodes in different order.
 * Therefore, the number of copysets (globally) can be higher than
 * ((scatterwidth / (replicationFactor − 1)) * (n / replicationFactor))
 * but still a lot smaller (for n >> scatterWidth) compared to random replication (n choose replicationFactor).
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 01.12.2017
 */
public class CopysetPlacement extends AbstractPlacementStrategy {
    private static final Logger LOGGER = LogManager.getFormatterLogger(CopysetPlacement.class);

    private BackupPeer[][] m_copysets;

    private BackupPeer[] m_incompleteCopyset;
    private int m_unallocatedPeers = 0;

    /**
     * Creates an instance of CopysetPlacement
     */
    public CopysetPlacement(final int p_replicationFactor, final boolean p_disjunctiveFirstBackupPeer,
            final boolean p_rackAware, final boolean p_switchAware) {
        super(p_replicationFactor, p_disjunctiveFirstBackupPeer, p_rackAware, p_switchAware);

        // ScatterWidth does not impact recovery performance -> like for RAMCloud the scatter width for DXRAM is
        // always (replicationFactor - 1)
        // Permutations are always 1 for fixed scatterWidth
        // (formula: (int) Math.ceil(scatterWidth / (m_replicationFactor - 1)))

        m_incompleteCopyset = new BackupPeer[m_replicationFactor];
    }

    @Override
    public boolean initialize(final List<BackupPeer> p_availablePeers) {
        checkAwarenessApplicability(p_availablePeers);

        int numberOfPeersAligned = p_availablePeers.size() - p_availablePeers.size() % m_replicationFactor;
        if (numberOfPeersAligned > m_replicationFactor) {
            m_copysets = new BackupPeer[numberOfPeersAligned / m_replicationFactor][m_replicationFactor];
            Random rand = new Random(123456789); // Use the same seed on all nodes to create identical copysets

            BackupPeer[] permutation = new BackupPeer[numberOfPeersAligned];
            // Calculate permutation
            BackupPeer currentPeer;
            BackupPeer cmp;
            boolean aware;
            int tries = 0;
            int counter = 0;
            while (counter < numberOfPeersAligned) {
                currentPeer = p_availablePeers.remove(rand.nextInt(p_availablePeers.size()));

                // Check rack- and switch-awareness
                aware = true;
                for (int i = counter - counter % m_replicationFactor; i < counter; i++) {
                    cmp = permutation[i];
                    if (m_rackAware && currentPeer.getRack() == cmp.getRack() ||
                            m_switchAware && currentPeer.getSwitch() == cmp.getSwitch()) {
                        aware = false;
                        break;
                    }
                }
                if (!aware) {
                    // Put the peer back and try again
                    p_availablePeers.add(currentPeer);
                    if (tries++ == 1000000) {
                        LOGGER.warn("Unable to find enough copysets meeting the requirements. Fallback to random " +
                                "replication!");

                        return false;
                    }
                    continue;
                }

                permutation[counter] = currentPeer;
                counter++;
                tries = 0;
            }

            // Determine copysets for current permutation
            for (int i = 0; i < numberOfPeersAligned / m_replicationFactor; i++) {
                m_copysets[i] = new BackupPeer[m_replicationFactor];
                System.arraycopy(permutation, i * m_replicationFactor, m_copysets[i], 0, m_replicationFactor);
            }
        }

        // Add peers that did not fit in copysets to possibly be complemented later
        if (!p_availablePeers.isEmpty()) {
            for (int i = 0; i < p_availablePeers.size(); i++) {
                m_incompleteCopyset[m_unallocatedPeers++] = p_availablePeers.get(i);
            }
        }

        return true;
    }

    @Override
    public void addNewBackupPeer(final BackupPeer p_newPeer) {
        if (m_copysets != null) {
            m_incompleteCopyset[m_unallocatedPeers++] = p_newPeer;
            if (m_unallocatedPeers == m_replicationFactor) {
                // Copyset is complete -> add (m_replicationFactor) new backup peers
                // Enlarge array by one copysets
                BackupPeer[][] tmp = new BackupPeer[m_copysets.length + 1][];
                System.arraycopy(m_copysets, 0, tmp, 0, m_copysets.length);

                // Add copyset with (m_replicationFactor) new backup peers
                tmp[m_copysets.length] = new BackupPeer[m_replicationFactor];
                System.arraycopy(m_incompleteCopyset, 0, tmp[m_copysets.length], 0, m_replicationFactor);
                m_copysets = tmp;

                Arrays.fill(m_incompleteCopyset, null);
                m_unallocatedPeers = 0;
            }
        } else {
            // Initial copysets not yet determined -> peer will be included automatically
        }
    }

    @Override
    public BackupPeer determineReplacementBackupPeer(final BackupPeer[] p_currentBackupPeers,
            final List<BackupPeer> p_availablePeers) {
        BackupPeer ret;
        BackupPeer currentPeer;
        short numberOfPeers;
        Random rand;

        numberOfPeers = (short) p_availablePeers.size();

        if (numberOfPeers < m_replicationFactor) {

            LOGGER.warn("Less than three peers for backup available. Replication will be incomplete!");

            return null;
        }

        if (numberOfPeers < m_replicationFactor * 2) {

            LOGGER.warn("Less than six peers for backup available. Some peers may store more" +
                    " than one backup range of a node!");

        }

        // Initialize Random with seed based on current backup peers to determine the same replacement on all peers
        int seed = 0;
        for (int i = 0; i < p_currentBackupPeers.length; i++) {
            if (p_currentBackupPeers[i] != null) {
                seed += p_currentBackupPeers[i].getNodeID();
            }
        }
        rand = new Random(seed);

        // Determine backup peer
        while (true) {
            // Choose random peer
            currentPeer = p_availablePeers.get(rand.nextInt(numberOfPeers));

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
        BackupPeer[] newBackupPeers;
        short numberOfPeers;
        Random rand = new Random(123456789);

        numberOfPeers = (short) p_availablePeers.size();
        newBackupPeers = new BackupPeer[m_replicationFactor];

        if (numberOfPeers < m_replicationFactor) {

            LOGGER.warn("Less than %d peers for backup available. Replication will be incomplete!",
                    m_replicationFactor);

            insufficientPeers = true;
        }

        BackupPeer firstBackupPeer;
        if (insufficientPeers) {
            if (numberOfPeers > 0) {
                // Determine backup peers randomly
                for (int i = 0; i < numberOfPeers; i++) {
                    addNewRandomBackupPeer(p_availablePeers, newBackupPeers, numberOfPeers, i, rand);
                }
            }
        } else {
            while (true) {
                if (m_disjunctive) {
                    // Determine first backup peer disjunctive
                    List<BackupPeer> availablePeers;
                    if (m_usedBackupPeers.size() < numberOfPeers) {
                        availablePeers = new ArrayList<BackupPeer>();
                        availablePeers.addAll(p_availablePeers);
                        availablePeers.removeAll(m_usedBackupPeers);
                    } else {
                        // Not enough backup peers available to be picky

                        LOGGER.warn("Insufficient peers available for disjunctive backup strategy." +
                                " Backup peers might be used more than once as a first backup peer!");

                        m_usedBackupPeers.clear();
                        availablePeers = p_availablePeers;
                    }

                    firstBackupPeer = availablePeers.get(rand.nextInt(availablePeers.size()));
                    m_usedBackupPeers.add(firstBackupPeer);
                } else {
                    // Determine first backup peer randomly
                    firstBackupPeer = p_availablePeers.get(rand.nextInt(numberOfPeers));
                }
                newBackupPeers[0] = firstBackupPeer;

                // Choose copyset containing the first backup peer
                BackupPeer[] copyset = null;
                for (int i = 0; i < m_copysets.length; i++) {
                    for (int j = 0; j < m_replicationFactor; j++) {
                        if (m_copysets[i][j].getNodeID() == firstBackupPeer.getNodeID()) {
                            copyset = m_copysets[i];
                            break;
                        }
                    }
                    if (copyset != null) {
                        break;
                    }
                }

                if (copyset != null) {
                    int counter = 1;
                    for (int i = 0; i < m_replicationFactor; i++) {
                        if (copyset[i].getNodeID() != firstBackupPeer.getNodeID()) {
                            newBackupPeers[counter++] = copyset[i];
                        }
                    }
                    break;
                }
            }
        }

        ret = new BackupRange(p_backupRangeID, newBackupPeers);

        return ret;
    }

    /**
     * Prints all copysets
     */
    private void printCopysets() {
        System.out.print("Determined copysets: ");
        for (BackupPeer[] peers : m_copysets) {
            System.out.print("{");
            for (BackupPeer peer : peers) {
                System.out.print("[" + peer.getNodeID() + ", " + peer.getRack() + ", " + peer.getSwitch() + "] ");
            }
            System.out.print("}");
        }
        System.out.println();
        System.out.print("Rest: {");
        for (BackupPeer peer : m_incompleteCopyset) {
            if (peer == null) {
                break;
            }
            System.out.print("[" + peer.getNodeID() + ", " + peer.getRack() + ", " + peer.getSwitch() + "] ");
        }
        System.out.println("}");
    }

}
