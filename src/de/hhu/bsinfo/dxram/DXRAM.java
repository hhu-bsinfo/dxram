/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Locale;

import de.hhu.bsinfo.dxram.app.ApplicationComponent;
import de.hhu.bsinfo.dxram.app.ApplicationService;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkAnonService;
import de.hhu.bsinfo.dxram.chunk.ChunkAsyncService;
import de.hhu.bsinfo.dxram.chunk.ChunkBackupComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkMemoryService;
import de.hhu.bsinfo.dxram.chunk.ChunkMigrationComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkRemoveService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.engine.NullComponent;
import de.hhu.bsinfo.dxram.engine.NullService;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.failure.FailureComponent;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.job.JobWorkStealingComponent;
import de.hhu.bsinfo.dxram.lock.PeerLockComponent;
import de.hhu.bsinfo.dxram.lock.PeerLockService;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.LogService;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupService;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.migration.MigrationService;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxram.recovery.RecoveryService;
import de.hhu.bsinfo.dxram.stats.StatisticsService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.dxutils.ManifestHelper;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Main class/entry point for DXRAM.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public final class DXRAM {
    public static final DXRAMVersion VERSION = new DXRAMVersion(0, 3, 0);
    public static final String GIT_COMMIT = "323dfa4"; //@GITCOMMIT@
    public static final String BUILD_TYPE = "debug"; //@BUILDTYPE@
    private static final String STARTUP_DONE_STR = "!---ooo---!";

    private DXRAMEngine m_engine;

    /**
     * Constructor
     */
    public DXRAM() {
        Locale.setDefault(new Locale("en", "US"));
        m_engine = new DXRAMEngine(VERSION);
        registerComponents(m_engine);
        registerServices(m_engine);
    }

    /**
     * Main entry point
     *
     * @param p_args
     *         Program arguments.
     */
    public static void main(final String[] p_args) {
        printJVMArgs();
        printCmdArgs(p_args);

        DXRAM dxram = new DXRAM();

        System.out.println("Starting DXRAM, version " + dxram.getVersion());

        if (!dxram.initialize(true)) {
            System.out.println("Initializing DXRAM failed.");
            System.exit(-1);
        }

        while (dxram.update()) {
            // run
        }

        System.exit(0);
    }

    /**
     * Get the version of DXRAM
     *
     * @return DXRAM version
     */
    public DXRAMVersion getVersion() {
        return m_engine.getVersion();
    }

    /**
     * Initialize the instance.
     *
     * @param p_autoShutdown
     *         True to have DXRAM shut down automatically when the application quits.
     *         If false, the caller has to take care of shutting down the instance by calling shutdown when done.
     * @return True if initializing was successful, false otherwise.
     */
    public boolean initialize(final boolean p_autoShutdown) {
        boolean ret = m_engine.init();
        if (!ret) {
            return false;
        }

        if (p_autoShutdown) {
            Runtime.getRuntime().addShutdownHook(new ShutdownThread(this));
        }

        printNodeInfo();

        // used for deploy script
        System.out.println(STARTUP_DONE_STR);

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
    public boolean update() {
        return m_engine.update();
    }

    /**
     * Shut down DXRAM. Call this if you have not enabled auto shutdown on init.
     */
    public void shutdown() {
        m_engine.shutdown();
    }

    /**
     * Register all default DXRAM components. If you want to register further components,
     * override this method but make sure to call it using super
     *
     * @param p_engine
     *         DXRAM engine instance to register components at
     */
    private static void registerComponents(final DXRAMEngine p_engine) {
        p_engine.registerComponent(ApplicationComponent.class);
        p_engine.registerComponent(BackupComponent.class);
        p_engine.registerComponent(ChunkComponent.class);
        p_engine.registerComponent(ChunkMigrationComponent.class);
        p_engine.registerComponent(ChunkBackupComponent.class);
        p_engine.registerComponent(EventComponent.class);
        p_engine.registerComponent(FailureComponent.class);
        p_engine.registerComponent(JobWorkStealingComponent.class);
        p_engine.registerComponent(LogComponent.class);
        p_engine.registerComponent(LookupComponent.class);
        p_engine.registerComponent(MemoryManagerComponent.class);
        p_engine.registerComponent(NameserviceComponent.class);
        p_engine.registerComponent(NetworkComponent.class);
        p_engine.registerComponent(NullComponent.class);
        p_engine.registerComponent(PeerLockComponent.class);
        p_engine.registerComponent(ZookeeperBootComponent.class);
    }

    /**
     * Register all default DXRAM services. If you want to register further services,
     * override this method but make sure to call it using super
     *
     * @param p_engine
     *         DXRAM engine instance to register services at
     */
    private static void registerServices(final DXRAMEngine p_engine) {
        p_engine.registerService(ApplicationService.class);
        p_engine.registerService(BootService.class);
        p_engine.registerService(ChunkAnonService.class);
        p_engine.registerService(ChunkAsyncService.class);
        p_engine.registerService(ChunkMemoryService.class);
        p_engine.registerService(ChunkRemoveService.class);
        p_engine.registerService(ChunkService.class);
        p_engine.registerService(JobService.class);
        p_engine.registerService(LogService.class);
        p_engine.registerService(LoggerService.class);
        p_engine.registerService(LookupService.class);
        p_engine.registerService(MasterSlaveComputeService.class);
        p_engine.registerService(MigrationService.class);
        p_engine.registerService(NameserviceService.class);
        p_engine.registerService(NetworkService.class);
        p_engine.registerService(NullService.class);
        p_engine.registerService(PeerLockService.class);
        p_engine.registerService(RecoveryService.class);
        p_engine.registerService(StatisticsService.class);
        p_engine.registerService(SynchronizationService.class);
        p_engine.registerService(TemporaryStorageService.class);
    }

    /**
     * Print some information after init about our current node.
     */
    private void printNodeInfo() {
        String str = ">>> DXRAM Node <<<\n";
        str += VERSION + "\n";
        str += "Build type: " + BUILD_TYPE + '\n';
        str += "Git commit: " + GIT_COMMIT + "\n";

        String buildDate = ManifestHelper.getProperty(getClass(), "BuildDate");
        if (buildDate != null) {
            str += "BuildDate: " + buildDate + '\n';
        }
        String buildUser = ManifestHelper.getProperty(getClass(), "BuildUser");
        if (buildUser != null) {
            str += "BuildUser: " + buildUser + '\n';
        }

        str += "Cwd: " + System.getProperty("user.dir") + '\n';

        BootService bootService = m_engine.getService(BootService.class);

        if (bootService != null) {
            short nodeId = bootService.getNodeID();
            str += "NodeID: " + NodeID.toHexString(nodeId) + '\n';
            str += "Role: " + bootService.getNodeRole(nodeId) + '\n';

            InetSocketAddress address = bootService.getNodeAddress(nodeId);
            str += "Address: " + address;

            System.out.println(str);
        }
    }

    /**
     * Print all cmd args specified on startup
     *
     * @param p_args
     *         Main arguments
     */
    private static void printCmdArgs(final String[] p_args) {
        StringBuilder builder = new StringBuilder();
        builder.append("Cmd arguments: ");

        for (String arg : p_args) {
            builder.append(arg);
            builder.append(' ');
        }

        System.out.println(builder);
    }

    /**
     * Print all JVM args specified on startup
     */
    private static void printJVMArgs() {
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        List<String> args = runtimeMxBean.getInputArguments();

        StringBuilder builder = new StringBuilder();
        builder.append("JVM arguments: ");

        for (String arg : args) {
            builder.append(arg);
            builder.append(' ');
        }

        System.out.println(builder);
    }

    /**
     * Shuts down DXRAM in case of the system exits
     *
     * @author Florian Klein 03.09.2013
     */
    private static final class ShutdownThread extends Thread {

        private DXRAM m_dxram;

        /**
         * Creates an instance of ShutdownThread
         *
         * @param p_dxram
         *         Reference to DXRAM instance.
         */
        private ShutdownThread(final DXRAM p_dxram) {
            super(ShutdownThread.class.getSimpleName());
            m_dxram = p_dxram;
        }

        @Override
        public void run() {
            m_dxram.shutdown();
        }

    }
}
