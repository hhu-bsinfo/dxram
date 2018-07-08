package de.hhu.bsinfo.dxram.monitoring.config;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.monitoring.MonitoringService;
import de.hhu.bsinfo.dxutils.OSValidator;

public class MonitoringServiceConfig extends AbstractDXRAMServiceConfig {

    public MonitoringServiceConfig() {
        super(MonitoringService.class, true, true);
    }

    @Override
    protected boolean verify(DXRAMContext.Config p_config) {
        if(!OSValidator.isUnix()) {
            // #if LOGGER == ERROR
            LOGGER.error("Monitoring is only supported for unix operating systems. Fix your configuration");
            // #endif /* LOGGER == ERROR */
            return false;
        }

        return true;
    }
}
