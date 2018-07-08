package de.hhu.bsinfo.dxram.monitoring;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.monitoring.config.MonitoringComponentConfig;

public class MonitoringComponent extends AbstractDXRAMComponent<MonitoringComponentConfig> {

    public MonitoringComponent() {
        super(DXRAMComponentOrder.Init.MONITORING, DXRAMComponentOrder.Shutdown.MONITORING, MonitoringComponentConfig.class);
    }

    @Override
    protected void resolveComponentDependencies(DXRAMComponentAccessor p_componentAccessor) {

    }

    @Override
    protected boolean initComponent(DXRAMContext.Config p_config) {
        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        return true;
    }

    @Override
    protected boolean supportsSuperpeer() {
        return true;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }
}
