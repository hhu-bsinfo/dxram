package de.hhu.bsinfo.dxram.backup;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

/**
 * Config for the BackupComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class BackupComponentConfig extends AbstractDXRAMComponentConfig {
    @Expose
    private boolean m_backupActive = false;

    @Expose
    private boolean m_availableForBackup = true;

    @Expose
    private String m_backupDirectory = "./log/";

    @Expose
    private StorageUnit m_backupRangeSize = new StorageUnit(256, StorageUnit.MB);

    @Expose
    private byte m_replicationFactor = 3;

    @Expose
    private String m_backupPlacementStrategy = "Random";

    @Expose
    private boolean m_disjunctiveFirstBackupPeer = true;

    @Expose
    private boolean m_rackAware = false;

    @Expose
    private boolean m_switchAware = false;

    /**
     * Constructor
     */
    public BackupComponentConfig() {
        super(BackupComponent.class, false, true);
    }

    /**
     * Activate/Disable the backup. This parameter should be either active for all nodes or inactive for all nodes
     */
    public boolean isBackupActive() {
        return m_backupActive;
    }

    /**
     * This parameter can be set to false for single peers to avoid storing backups and the associated overhead.
     * If this peer is not available for backup, it will not log and recover chunks but all other backup functions,
     * like replicating own chunks, are enabled.
     * Do not set this parameter globally to deactivate backup. Use backupActive parameter for that purpose.
     */
    boolean availableForBackup() {
        return m_availableForBackup;
    }

    /**
     * Directory where the backup data is stored
     */
    public String getBackupDirectory() {
        return m_backupDirectory;
    }

    /**
     * Size of a backup range
     */
    public StorageUnit getBackupRangeSize() {
        return m_backupRangeSize;
    }

    /**
     * The replication factor must must be in [1, 4], for disabling replication set m_backupActive to false
     */
    public byte getReplicationFactor() {
        return m_replicationFactor;
    }

    /**
     * The backup placement strategy
     * "Random": all backup peers are selected randomly
     * "Copyset": copyset replication
     * "LocationAware"
     */
    String getBackupPlacementStrategy() {
        return m_backupPlacementStrategy;
    }

    /**
     * If true, the first backup peer is selected disjunctive
     */
    boolean disjunctiveFirstBackupPeer() {
        return m_disjunctiveFirstBackupPeer;
    }

    /**
     * If true, in every set of backup peers all backup peers are from another rack, if possible
     */
    boolean rackAware() {
        return m_rackAware;
    }

    /**
     * If true, in every set of backup peers all backup peers are behind another switch, if possible
     */
    boolean switchAware() {
        return m_switchAware;
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {

        if (m_replicationFactor < 1 || m_replicationFactor > 4) {

            LOGGER.error("Replication factor must be in [1, 4]!");

            return false;
        }

        return true;
    }
}
