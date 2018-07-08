package de.hhu.bsinfo.dxram.monitoring;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.monitoring.config.MonitoringServiceConfig;

public class MonitoringService extends AbstractDXRAMService<MonitoringServiceConfig> {


    public MonitoringService() {
        super("monitoring", MonitoringServiceConfig.class);
    }

    @Override
    protected void resolveComponentDependencies(DXRAMComponentAccessor p_componentAccessor) {

    }

    @Override
    protected boolean startService(DXRAMContext.Config p_config) {
        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }

    @Override
    protected boolean supportsSuperpeer() {
        return true;
    }

    @Override
    protected boolean supportsPeer() {
        return false;
    }
}
