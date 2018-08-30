package de.hhu.bsinfo.dxram;

import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentManager;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMContextCreator;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceManager;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;

public class DXRAMTestContextCreator implements DXRAMContextCreator {
    private final IPV4Unit m_zookeeperConnection;
    private final DXRAMRunnerConfiguration m_config;
    private final int m_nodeIdx;
    private final int m_nodePort;

    public DXRAMTestContextCreator(final IPV4Unit p_zookeeperConnection, final DXRAMRunnerConfiguration p_config,
            final int p_nodeIdx, final int p_nodePort) {
        m_zookeeperConnection = p_zookeeperConnection;
        m_config = p_config;
        m_nodeIdx = p_nodeIdx;
        m_nodePort = p_nodePort;
    }

    @Override
    public DXRAMContext create(final DXRAMComponentManager p_componentManager,
            final DXRAMServiceManager p_serviceManager) {
        DXRAMContext context = new DXRAMContext();
        context.createDefaultComponents(p_componentManager);
        context.createDefaultServices(p_serviceManager);

        context.getConfig().getEngineConfig().setRole(m_config.nodes()[m_nodeIdx].nodeRole().toString());
        context.getConfig().getEngineConfig().setAddress(new IPV4Unit("127.0.0.1", m_nodePort));

        context.getConfig().getComponentConfig(ZookeeperBootComponentConfig.class).setConnection(
                m_zookeeperConnection);

        return context;
    }
}
