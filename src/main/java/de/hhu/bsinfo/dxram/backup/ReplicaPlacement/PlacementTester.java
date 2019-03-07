package de.hhu.bsinfo.dxram.backup.ReplicaPlacement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.DXNetMain;
import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.backup.BackupRange;

/**
 * Class for testing the placement strategies.
 */
public final class PlacementTester {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXNetMain.class);

    /**
     * Unused constructor.
     */
    private PlacementTester() {
    }

    /**
     * Main
     *
     * @param p_args
     *         program arguments
     */
    public static void main(String[] p_args) {
        int replicationFactor;
        boolean disjunctive;
        boolean rackAware;
        boolean switchAware;
        int racks;
        int switches;

        // Parse command line arguments
        if (p_args.length != 4 && p_args.length != 10) {
            System.out.println("Usage: num_peers num_backup_ranges num_new_peers num_replacements " +
                    "[replication_factor disjunctive rackAware switchAware num_racks num_switches]");
            System.exit(-1);
        }

        int peers = Integer.parseInt(p_args[0]);
        int backupRanges = Integer.parseInt(p_args[1]);
        int newPeers = Integer.parseInt(p_args[2]);
        int replacements = Integer.parseInt(p_args[3]);
        if (p_args.length == 4) {
            replicationFactor = 3;
            disjunctive = false;
            rackAware = false;
            switchAware = false;
            racks = 100;
            switches = 10;
        } else {
            replicationFactor = Integer.parseInt(p_args[4]);
            disjunctive = Boolean.parseBoolean(p_args[5]);
            rackAware = Boolean.parseBoolean(p_args[6]);
            switchAware = Boolean.parseBoolean(p_args[7]);
            racks = Integer.parseInt(p_args[8]);
            switches = Integer.parseInt(p_args[9]);
        }

        Random rand = new Random(0);
        ArrayList<BackupPeer> availablePeers = new ArrayList<BackupPeer>(peers);
        for (int i = 0; i < peers; i++) {
            availablePeers.add(new BackupPeer((short) rand.nextInt(), (short) rand.nextInt(racks),
                    (short) rand.nextInt(switches)));
        }

        ArrayList<BackupPeer> initialSetOfPeers = new ArrayList<>(availablePeers.size());
        initialSetOfPeers.addAll(availablePeers);

        /* Replace class here to switch placement strategy */
        AbstractPlacementStrategy strategy = new CopysetPlacement(replicationFactor, disjunctive, rackAware,
                switchAware);
        if (!strategy.initialize(initialSetOfPeers)) {
            strategy = new RandomPlacement(replicationFactor, disjunctive, rackAware, switchAware);
        }

        final AbstractPlacementStrategy strat = strategy;
        Runnable task = () -> {
            LOGGER.info("Determine %d backup ranges for initial set of peers.", backupRanges);
            BackupRange oldBackupRange = null;
            BackupRange newBackupRange;
            for (int i = 0; i < backupRanges; i++) {
                newBackupRange = strat.determineBackupPeers((short) i, availablePeers, oldBackupRange);

                BackupPeer[] backupPeers = newBackupRange.getBackupPeers();
                for (int j = 0; j < replicationFactor; j++) {
                    if (backupPeers[j] == null) {
                        LOGGER.warn("Backup peer is null!");
                    }
                    if (rackAware) {
                        for (int k = 0; k < j; k++) {
                            if (backupPeers[k].getRack() == backupPeers[j].getRack()) {
                                LOGGER.error("Backup peers %d %d are in the same rack!", k, j);
                            }
                        }
                    }
                    if (switchAware) {
                        for (int k = 0; k < j; k++) {
                            if (backupPeers[k].getSwitch() == backupPeers[j].getSwitch()) {
                                LOGGER.error("Backup peers %d %d are connected to the same switch!", k, j);
                            }
                        }
                    }
                }

                oldBackupRange = newBackupRange;
            }

            LOGGER.info("Adding %d new backup peers.", newPeers);
            for (int i = 0; i < newPeers; i++) {
                strat.addNewBackupPeer(new BackupPeer((short) rand.nextInt(), (short) rand.nextInt(racks),
                        (short) rand.nextInt(switches)));
            }

            LOGGER.info("Replacing %d backup peers.", replacements);
            BackupPeer[] test = new BackupPeer[replicationFactor];
            Arrays.fill(test, null);
            for (int i = 0; i < replacements; i++) {
                strat.determineReplacementBackupPeer(test, availablePeers);
            }

            LOGGER.info("Determine %d backup ranges for modified set of peers.", backupRanges);
            for (int i = 0; i < backupRanges; i++) {
                newBackupRange = strat.determineBackupPeers((short) i, availablePeers, oldBackupRange);

                BackupPeer[] backupPeers = newBackupRange.getBackupPeers();
                for (int j = 0; j < replicationFactor; j++) {
                    if (backupPeers[j] == null) {
                        LOGGER.warn("Backup peer is null!");
                    }
                    if (rackAware) {
                        for (int k = 0; k < j; k++) {
                            if (backupPeers[k].getRack() == backupPeers[j].getRack()) {
                                LOGGER.error("Backup peers %d %d are in the same rack!", k, j);
                            }
                        }
                    }
                    if (switchAware) {
                        for (int k = 0; k < j; k++) {
                            if (backupPeers[k].getSwitch() == backupPeers[j].getSwitch()) {
                                LOGGER.error("Backup peers %d %d are connected to the same switch!", k, j);
                            }
                        }
                    }
                }

                oldBackupRange = newBackupRange;
            }
        };

        long timeStart = System.nanoTime();
        LOGGER.info("Starting workload...");
        Thread thread = new Thread(task);
        thread.start();

        try {
            thread.join();
        } catch (InterruptedException ignore) {
        }
        LOGGER.info("Workload finished on sender.");

        long timeDiff = System.nanoTime() - timeStart;
        LOGGER.info("Runtime: %d ms", timeDiff / 1000 / 1000);
    }
}
