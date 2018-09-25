package de.hhu.bsinfo.dxram.log;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxlog.DXLogConfig;
import de.hhu.bsinfo.dxram.backup.BackupComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the LogComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
@DXRAMComponentConfig.Settings(component = LogComponent.class, supportsSuperpeer = false, supportsPeer = true)
public class LogComponentConfig extends DXRAMComponentConfig {

    /**
     * Get the dxlog configuration values
     */
    @Expose
    private DXLogConfig m_dxlogConfig = new DXLogConfig();

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return m_dxlogConfig
                .verify((int) p_config.getComponentConfig(BackupComponentConfig.class).getBackupRangeSize().getBytes() *
                        2);
    }
}
