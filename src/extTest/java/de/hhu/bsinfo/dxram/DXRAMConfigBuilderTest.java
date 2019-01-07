package de.hhu.bsinfo.dxram;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupComponentConfig;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponent;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilder;
import de.hhu.bsinfo.dxram.job.JobComponent;
import de.hhu.bsinfo.dxram.job.JobComponentConfig;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;

/**
 * Creates a configuration for a DXRAM runner. This allows runtime configuration of settings for the DXRAM
 * instances to start for the tests to run.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
class DXRAMConfigBuilderTest implements DXRAMConfigBuilder {
    private final String m_dxramBuildDistDir;
    private final IPV4Unit m_zookeeperConnection;
    private final DXRAMTestConfiguration m_config;
    private final int m_nodeIdx;
    private final int m_nodePort;

    /**
     * Constructor
     *
     * @param p_dxramBuilDistDir
     *         Path to directory containing the build output for distribution (required to test with applications,
     *         backup/logging etc)
     * @param p_zookeeperConnection
     *         Address of the zookeeper server instance to connect to
     * @param p_config
     *         Configuration for the test class to run
     * @param p_nodeIdx
     *         Index of node to configure
     * @param p_nodePort
     *         Port to assign to node
     */
    DXRAMConfigBuilderTest(final String p_dxramBuilDistDir, final IPV4Unit p_zookeeperConnection,
            final DXRAMTestConfiguration p_config, final int p_nodeIdx, final int p_nodePort) {
        m_dxramBuildDistDir = p_dxramBuilDistDir;
        m_zookeeperConnection = p_zookeeperConnection;
        m_config = p_config;
        m_nodeIdx = p_nodeIdx;
        m_nodePort = p_nodePort;
    }

    @Override
    public DXRAMConfig build(final DXRAMConfig p_config) {
        p_config.getEngineConfig().setJniPath(m_dxramBuildDistDir + "/jni");

        p_config.getEngineConfig().setRole(m_config.nodes()[m_nodeIdx].nodeRole().toString());
        p_config.getEngineConfig().setAddress(new IPV4Unit("127.0.0.1", m_nodePort));

        ZookeeperBootComponentConfig zookeeperConfig = p_config.getComponentConfig(ZookeeperBootComponent.class);
        zookeeperConfig.setConnection(m_zookeeperConnection);

        BackupComponentConfig backupConfig = p_config.getComponentConfig(BackupComponent.class);
        backupConfig.setBackupActive(m_config.nodes()[m_nodeIdx].backupActive());
        backupConfig.setAvailableForBackup(m_config.nodes()[m_nodeIdx].availableForBackup());

        JobComponentConfig jobConfig = p_config.getComponentConfig(JobComponent.class);
        jobConfig.setEnabled(m_config.nodes()[m_nodeIdx].enableJobService());

        return p_config;
    }
}
