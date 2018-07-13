package de.hhu.bsinfo.dxram.monitoring;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxutils.OSValidator;

public class MonitoringServiceConfig extends AbstractDXRAMServiceConfig {

    public MonitoringServiceConfig() {
        // FIXME temporarily disable the component by default due to several bugs that must be fixed first
        super(MonitoringService.class, false, false);
        //super(MonitoringService.class, true, true);
    }

    @Override
    protected boolean verify(DXRAMContext.Config p_config) {
        if (!OSValidator.isUnix()) {
            LOGGER.error("Monitoring is only supported for unix operating systems. Fix your configuration");
            return false;
        }

        return true;
    }
}
