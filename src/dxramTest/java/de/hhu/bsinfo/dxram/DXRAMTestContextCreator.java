package de.hhu.bsinfo.dxram;

import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentManager;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMContextCreator;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceManager;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;

/**
 * Creates a DXRAMContext for the DXRAM runner. This allows runtime configuration of settings for the DXRAM
 * instances to start for the tests to run.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
class DXRAMTestContextCreator implements DXRAMContextCreator {
    private final IPV4Unit m_zookeeperConnection;
    private final DXRAMTestConfiguration m_config;
    private final int m_nodeIdx;
    private final int m_nodePort;

    /**
     * Constructor
     *
     * @param p_zookeeperConnection
     *         Address of the zookeeper server instance to connect to
     * @param p_config
     *         Configuration for the test class to run
     * @param p_nodeIdx
     *         Index of node to configure
     * @param p_nodePort
     *         Port to assign to node
     */
    DXRAMTestContextCreator(final IPV4Unit p_zookeeperConnection, final DXRAMTestConfiguration p_config,
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
