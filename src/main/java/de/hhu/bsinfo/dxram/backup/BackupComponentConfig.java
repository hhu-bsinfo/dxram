package de.hhu.bsinfo.dxram.backup;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMModuleConfig;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

/**
 * Config for the BackupComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
public class BackupComponentConfig extends DXRAMModuleConfig {
    /**
     * Activate/Disable the backup. This parameter should be either active for all nodes or inactive for all nodes
     */
    @Expose
    private boolean m_backupActive = false;

    /**
     * This parameter can be set to false for single peers to avoid storing backups and the associated overhead.
     * If this peer is not available for backup, it will not log and recover chunks but all other backup functions,
     * like replicating own chunks, are enabled.
     * Do not set this parameter globally to deactivate backup. Use backupActive parameter for that purpose.
     */
    @Expose
    private boolean m_availableForBackup = true;

    /**
     * Directory where the backup data is stored
     */
    @Expose
    private String m_backupDirectory = "./log/";

    /**
     * Size of a backup range
     */
    @Expose
    private StorageUnit m_backupRangeSize = new StorageUnit(256, StorageUnit.MB);

    /**
     * The replication factor must must be in [1, 4], for disabling replication set m_backupActive to false
     */
    @Expose
    private byte m_replicationFactor = 3;

    /**
     * The backup placement strategy
     * "Random": all backup peers are selected randomly
     * "Copyset": copyset replication
     * "LocationAware"
     */
    @Expose
    private String m_backupPlacementStrategy = "Random";

    /**
     * If true, the first backup peer is selected disjunctive
     */
    @Expose
    private boolean m_disjunctiveFirstBackupPeer = true;

    /**
     * If true, in every set of backup peers all backup peers are from another rack, if possible
     */
    @Expose
    private boolean m_rackAware = false;

    /**
     * If true, in every set of backup peers all backup peers are behind another switch, if possible
     */
    @Expose
    private boolean m_switchAware = false;

    /**
     * Constructor
     */
    public BackupComponentConfig() {
        super(BackupComponent.class);
    }

    @Override
    protected boolean verify(final DXRAMConfig p_config) {
        if (m_replicationFactor < 1 || m_replicationFactor > 4) {
            LOGGER.error("Replication factor must be in [1, 4]!");

            return false;
        }

        return true;
    }
}
