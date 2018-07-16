package de.hhu.bsinfo.dxram.monitoring;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxutils.OSValidator;

public class MonitoringServiceConfig extends AbstractDXRAMServiceConfig {

    @Expose
    private boolean m_monitoringActive = false;

    public MonitoringServiceConfig() {
        super(MonitoringService.class, true, true);
    }

    @Override
    protected boolean verify(DXRAMContext.Config p_config) {
        if (!OSValidator.isUnix()) {
            LOGGER.error("Monitoring is only supported for unix operating systems. Fix your configuration");
            return false;
        }

        return true;
    }

    public boolean isMonitoringActive() {
        return m_monitoringActive;
    }
}
