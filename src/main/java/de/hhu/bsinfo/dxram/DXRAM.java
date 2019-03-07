/*
 * Copyright (C) 2019 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.Locale;
import java.util.stream.Collectors;

import de.hhu.bsinfo.dxmonitor.info.InstanceInfo;
import de.hhu.bsinfo.dxram.app.ApplicationComponent;
import de.hhu.bsinfo.dxram.app.ApplicationService;
import de.hhu.bsinfo.dxram.app.ApplicationServiceConfig;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupComponentConfig;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponent;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponentConfig;
import de.hhu.bsinfo.dxram.chunk.ChunkAnonService;
import de.hhu.bsinfo.dxram.chunk.ChunkBackupComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponentConfig;
import de.hhu.bsinfo.dxram.chunk.ChunkDebugService;
import de.hhu.bsinfo.dxram.chunk.ChunkIndexComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkMigrationComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.ChunkServiceConfig;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.engine.DXRAMModuleConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.engine.NullComponent;
import de.hhu.bsinfo.dxram.engine.NullService;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.event.EventComponentConfig;
import de.hhu.bsinfo.dxram.failure.FailureComponent;
import de.hhu.bsinfo.dxram.function.FunctionService;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import de.hhu.bsinfo.dxram.job.JobComponent;
import de.hhu.bsinfo.dxram.job.JobComponentConfig;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.LogComponentConfig;
import de.hhu.bsinfo.dxram.log.LogService;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupComponentConfig;
import de.hhu.bsinfo.dxram.lookup.LookupService;
import de.hhu.bsinfo.dxram.management.ManagementService;
import de.hhu.bsinfo.dxram.management.ManagementServiceConfig;
import de.hhu.bsinfo.dxram.migration.MigrationService;
import de.hhu.bsinfo.dxram.monitoring.MonitoringComponent;
import de.hhu.bsinfo.dxram.monitoring.MonitoringComponentConfig;
import de.hhu.bsinfo.dxram.monitoring.MonitoringService;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeServiceConfig;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponentConfig;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponentConfig;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxram.plugin.PluginComponent;
import de.hhu.bsinfo.dxram.plugin.PluginComponentConfig;
import de.hhu.bsinfo.dxram.recovery.RecoveryService;
import de.hhu.bsinfo.dxram.stats.StatisticsService;
import de.hhu.bsinfo.dxram.stats.StatisticsServiceConfig;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import de.hhu.bsinfo.dxram.sync.SynchronizationServiceConfig;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageServiceConfig;
import de.hhu.bsinfo.dxram.util.NodeCapabilities;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * DXRAM main class (for main entry point, refer to DXRAMMain).
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public final class DXRAM {


    private DXRAMEngine m_engine;
    private ShutdownThread m_shutdownHook;

    public static final String EXTENDED_TEST_PROPERTY = "extendedTest";

    public static final String VERBOSE_PROPERTY = "verbose";

    public static final String BASE_DIR_PROPERTY = "dxram.baseDir";

    public static final String BASE_DIR = System.getProperty(BASE_DIR_PROPERTY, "");

    private static final String BANNER_FILENAME = "banner.txt";

    /**
     * Constructor.
     */
    public DXRAM() {
        Locale.setDefault(new Locale("en", "US"));

        if (!isRunWithinTest() && isVerbose()) {
            printBuildInfo();
            printInstanceInfo();
        }

        m_engine = new DXRAMEngine(BuildConfig.DXRAM_VERSION);
        registerComponents(m_engine);
        registerServices(m_engine);
    }

    /**
     * Get the version of DXRAM.
     *
     * @return DXRAM version
     */
    public DXRAMVersion getVersion() {
        return m_engine.getVersion();
    }

    /**
     * Create a configuration instance with default values.
     *
     * @return Configuration instance
     */
    public DXRAMConfig createDefaultConfigInstance() {
        return m_engine.createConfigInstance();
    }

    /**
     * Initialize the instance.
     *
     * @param p_config
     *         Configuration to use for instance
     * @param p_autoShutdown
     *         True to have DXRAM shut down automatically when the application quits.
     *         If false, the caller has to take care of shutting down the instance by calling shutdown when done.
     * @return True if initializing was successful, false otherwise.
     */
    public boolean initialize(final DXRAMConfig p_config, final boolean p_autoShutdown) {
        boolean ret = m_engine.init(p_config);
        if (!ret) {
            return false;
        }

        if (p_autoShutdown) {
            m_shutdownHook = new ShutdownThread(this);
            Runtime.getRuntime().addShutdownHook(m_shutdownHook);
        }

        if (isRunWithinTest() && !isVerbose()) {
            printNodeInfo();
        }

        return true;
    }

    /**
     * Get a service from DXRAM.
     *
     * @param <T>
     *         Type of service to get
     * @param p_class
     *         Class of the service to get. If one service has multiple implementations, use
     *         the common super class here.
     * @return Service requested or null if the service is not enabled/available.
     */
    public <T extends AbstractDXRAMService> T getService(final Class<T> p_class) {
        return m_engine.getService(p_class);
    }

    /**
     * Update the engine. This must be called by the main thread
     *
     * @return True on success, false on failure
     */
    public void run() {
        m_engine.run();
    }

    /**
     * Shut down DXRAM. Call this if you have not enabled auto shutdown on init.
     */
    public void shutdown() {
        if (m_shutdownHook != null) {
            Runtime.getRuntime().removeShutdownHook(m_shutdownHook);
            m_shutdownHook = null;
        }

        m_engine.shutdown();
    }

    /**
     * Register all DXRAM components.
     *
     * @param p_engine
     *         DXRAM engine instance to register components at
     */
    private static void registerComponents(final DXRAMEngine p_engine) {
        p_engine.registerComponent(ApplicationComponent.class, DXRAMModuleConfig.class);
        p_engine.registerComponent(BackupComponent.class, BackupComponentConfig.class);
        p_engine.registerComponent(ChunkBackupComponent.class, DXRAMModuleConfig.class);
        p_engine.registerComponent(ChunkComponent.class, ChunkComponentConfig.class);
        p_engine.registerComponent(ChunkIndexComponent.class, DXRAMModuleConfig.class);
        p_engine.registerComponent(ChunkMigrationComponent.class, DXRAMModuleConfig.class);
        p_engine.registerComponent(MonitoringComponent.class, MonitoringComponentConfig.class);
        p_engine.registerComponent(EventComponent.class, EventComponentConfig.class);
        p_engine.registerComponent(FailureComponent.class, DXRAMModuleConfig.class);
        p_engine.registerComponent(JobComponent.class, JobComponentConfig.class);
        p_engine.registerComponent(LogComponent.class, LogComponentConfig.class);
        p_engine.registerComponent(LookupComponent.class, LookupComponentConfig.class);
        p_engine.registerComponent(NameserviceComponent.class, NameserviceComponentConfig.class);
        p_engine.registerComponent(NetworkComponent.class, NetworkComponentConfig.class);
        p_engine.registerComponent(NullComponent.class, DXRAMModuleConfig.class);
        p_engine.registerComponent(PluginComponent.class, PluginComponentConfig.class);
        p_engine.registerComponent(ZookeeperBootComponent.class, ZookeeperBootComponentConfig.class);
    }

    /**
     * Register all DXRAM services.
     *
     * @param p_engine
     *         DXRAM engine instance to register services at
     */
    private static void registerServices(final DXRAMEngine p_engine) {
        p_engine.registerService(ApplicationService.class, ApplicationServiceConfig.class);
        p_engine.registerService(BootService.class, DXRAMModuleConfig.class);
        p_engine.registerService(ChunkAnonService.class, DXRAMModuleConfig.class);
        p_engine.registerService(ChunkDebugService.class, DXRAMModuleConfig.class);
        p_engine.registerService(ChunkLocalService.class, DXRAMModuleConfig.class);
        p_engine.registerService(ChunkService.class, ChunkServiceConfig.class);
        p_engine.registerService(JobService.class, DXRAMModuleConfig.class);
        p_engine.registerService(LogService.class, DXRAMModuleConfig.class);
        p_engine.registerService(LoggerService.class, DXRAMModuleConfig.class);
        p_engine.registerService(LookupService.class, DXRAMModuleConfig.class);
        p_engine.registerService(MasterSlaveComputeService.class, MasterSlaveComputeServiceConfig.class);
        p_engine.registerService(MigrationService.class, DXRAMModuleConfig.class);
        p_engine.registerService(MonitoringService.class, DXRAMModuleConfig.class);
        p_engine.registerService(NameserviceService.class, DXRAMModuleConfig.class);
        p_engine.registerService(NetworkService.class, DXRAMModuleConfig.class);
        p_engine.registerService(NullService.class, DXRAMModuleConfig.class);
        p_engine.registerService(RecoveryService.class, DXRAMModuleConfig.class);
        p_engine.registerService(StatisticsService.class, StatisticsServiceConfig.class);
        p_engine.registerService(SynchronizationService.class, SynchronizationServiceConfig.class);
        p_engine.registerService(TemporaryStorageService.class, TemporaryStorageServiceConfig.class);
        p_engine.registerService(FunctionService.class, DXRAMModuleConfig.class);
        p_engine.registerService(ManagementService.class, ManagementServiceConfig.class);
    }

    /**
     * Print information about the current build.
     */
    private static void printBuildInfo() {
        StringBuilder builder = new StringBuilder();

        builder.append(">>> DXRAM build <<<\n");
        builder.append(BuildConfig.DXRAM_VERSION);
        builder.append('\n');
        builder.append("Build type: ");
        builder.append(BuildConfig.BUILD_TYPE);
        builder.append('\n');
        builder.append("Git commit: ");
        builder.append(BuildConfig.GIT_COMMIT);
        builder.append('\n');
        builder.append("BuildDate: ");
        builder.append(BuildConfig.BUILD_DATE);
        builder.append('\n');
        builder.append("BuildUser: ");
        builder.append(BuildConfig.BUILD_USER);
        builder.append('\n');

        System.out.println(builder);
    }

    /**
     * Print some info about the current instance to the terminal.
     */
    private static void printInstanceInfo() {
        System.out.println(">>> Instance <<<\n" + InstanceInfo.compile() + '\n');
    }

    /**
     * Print some information after init about our current node.
     */
    private void printNodeInfo() {
        BootService bootService = m_engine.getService(BootService.class);

        StringBuilder builder = new StringBuilder();

        builder.append(">>> DXRAM node <<<\n");

        if (bootService != null) {
            short nodeId = bootService.getNodeID();
            int capabilities = bootService.getNodeCapabilities(nodeId);

            builder.append("NodeID: ");
            builder.append(NodeID.toHexString(nodeId));
            builder.append('\n');

            builder.append("Capabilities: ");
            builder.append(NodeCapabilities.toString(capabilities));
            builder.append('\n');

            builder.append("Role: ");
            builder.append(bootService.getNodeRole(nodeId));
            builder.append('\n');

            InetSocketAddress address = bootService.getNodeAddress(nodeId);
            builder.append("Address: ");
            builder.append(address);
            builder.append('\n');
        } else {
            builder.append("ERROR retrieving node info\n");
        }

        System.out.println(builder);
    }

    public static boolean isRunWithinTest() {
        return System.getProperty(EXTENDED_TEST_PROPERTY) != null;
    }

    public static boolean isVerbose() {
        return System.getProperty(VERBOSE_PROPERTY) != null;
    }

    public static void printBanner() {
        InputStream inputStream = DXRAM.class.getClassLoader().getResourceAsStream(BANNER_FILENAME);

        if (inputStream == null) {
            return;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String banner = reader.lines().collect(Collectors.joining(System.lineSeparator()));

        System.out.print("\n");
        System.out.printf(banner, BuildConfig.DXRAM_VERSION, BuildConfig.BUILD_DATE, BuildConfig.BUILD_TYPE,
                BuildConfig.BUILD_USER, BuildConfig.GIT_COMMIT);
        System.out.print("\n\n");
    }

    public static String getAbsolutePath(String p_directory) {
        return String.format("%s/%s", BASE_DIR, p_directory);
    }

    /**
     * Shuts down DXRAM in case of the system exits.
     *
     * @author Florian Klein 03.09.2013
     */
    private static final class ShutdownThread extends Thread {
        private DXRAM m_dxram;

        /**
         * Creates an instance of ShutdownThread.
         *
         * @param p_dxram
         *         Reference to DXRAM instance.
         */
        private ShutdownThread(final DXRAM p_dxram) {
            super("kill");
            m_dxram = p_dxram;
        }

        @Override
        public void run() {
            m_dxram.m_engine.shutdown();
        }

    }
}
