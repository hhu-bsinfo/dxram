package de.hhu.bsinfo.dxram;

import java.io.File;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupComponentConfig;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponent;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponentConfig;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilder;
import de.hhu.bsinfo.dxram.job.JobComponent;
import de.hhu.bsinfo.dxram.job.JobComponentConfig;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeServiceConfig;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponentConfig;
import de.hhu.bsinfo.dxram.plugin.PluginComponent;
import de.hhu.bsinfo.dxram.plugin.PluginComponentConfig;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;
import de.hhu.bsinfo.dxutils.unit.TimeUnit;

/**
 * Creates a configuration for a DXRAM runner. This allows runtime configuration of settings for the DXRAM
 * instances to start for the tests to run.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
class DXRAMConfigBuilderTest implements DXRAMConfigBuilder {
    private final String m_dxramBuildDistDir;
    private final DXRAMTestConfiguration m_config;
    private final int m_nodeIdx;
    private final int m_nodePort;

    /**
     * Constructor
     *
     * @param p_dxramBuilDistDir
     *         Path to directory containing the build output for distribution (required to test with applications,
     *         backup/logging etc)
     * @param p_config
     *         Configuration for the test class to run
     * @param p_nodeIdx
     *         Index of node to configure
     * @param p_nodePort
     *         Port to assign to node
     */
    DXRAMConfigBuilderTest(final String p_dxramBuilDistDir, final DXRAMTestConfiguration p_config, final int p_nodeIdx,
            final int p_nodePort) {
        m_dxramBuildDistDir = p_dxramBuilDistDir;
        m_config = p_config;
        m_nodeIdx = p_nodeIdx;
        m_nodePort = p_nodePort;
    }

    @Override
    public DXRAMConfig build(final DXRAMConfig p_config) {
        p_config.getEngineConfig().setJniPath(m_dxramBuildDistDir + File.separator + "jni");

        p_config.getEngineConfig().setRole(m_config.nodes()[m_nodeIdx].nodeRole().toString());
        p_config.getEngineConfig().setAddress(new IPV4Unit("127.0.0.1", m_nodePort));

        if (m_nodeIdx == 0) {
            ZookeeperBootComponentConfig bootConfig = p_config.getComponentConfig(ZookeeperBootComponent.class);
            bootConfig.setBootstrap(true);
            bootConfig.setDataDir(m_dxramBuildDistDir + File.separator + bootConfig.getDataDir());
        }

        PluginComponentConfig pluginConfig = p_config.getComponentConfig(PluginComponent.class);
        pluginConfig.setPluginsPath(m_dxramBuildDistDir + File.separator + "plugin");

        BackupComponentConfig backupConfig = p_config.getComponentConfig(BackupComponent.class);
        backupConfig.setBackupActive(m_config.nodes()[m_nodeIdx].backupActive());
        backupConfig.setAvailableForBackup(m_config.nodes()[m_nodeIdx].availableForBackup());

        JobComponentConfig jobConfig = p_config.getComponentConfig(JobComponent.class);
        jobConfig.setEnabled(m_config.nodes()[m_nodeIdx].enableJobService());

        ChunkComponentConfig chunkConfig = p_config.getComponentConfig(ChunkComponent.class);
        chunkConfig.setKeyValueStoreSize(new StorageUnit(m_config.nodes()[m_nodeIdx].keyValueStorageSizeMB(),
                StorageUnit.MB));
        chunkConfig.setChunkStorageEnabled(m_config.nodes()[m_nodeIdx].enableKeyValueStorage());

        MasterSlaveComputeServiceConfig masterSlaveConfig = p_config.getServiceConfig(MasterSlaveComputeService.class);
        masterSlaveConfig.setRole(m_config.nodes()[m_nodeIdx].masterSlaveComputeRole().toString());

        NetworkComponentConfig networkConfig = p_config.getComponentConfig(NetworkComponent.class);
        networkConfig.getNioConfig().setRequestTimeOut(
                new TimeUnit(m_config.nodes()[m_nodeIdx].networkRequestResponseTimeoutMs(), TimeUnit.MS));

        return p_config;
    }
}
