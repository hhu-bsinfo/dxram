package de.hhu.bsinfo.dxram.log;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxlog.DXLogConfig;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.ModuleConfig;

/**
 * Config for the LogComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
public class LogComponentConfig extends ModuleConfig {
    /**
     * Get the dxlog configuration values
     */
    @Expose
    private DXLogConfig m_dxlogConfig = new DXLogConfig();

    /**
     * Constructor
     */
    public LogComponentConfig() {
        super(LogComponent.class);
    }

    @Override
    protected boolean verify(final DXRAMConfig p_config) {
        BackupComponentConfig backupConfig = p_config.getComponentConfig(BackupComponent.class);

        return m_dxlogConfig.verify((int) backupConfig.getBackupRangeSize().getBytes() * 2);
    }
}
