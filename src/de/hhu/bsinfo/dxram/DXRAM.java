package de.hhu.bsinfo.dxram;

import java.net.InetSocketAddress;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponent;
import de.hhu.bsinfo.dxram.chunk.AsyncChunkService;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkMemoryService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.engine.NullComponent;
import de.hhu.bsinfo.dxram.engine.NullService;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.failure.FailureComponent;
import de.hhu.bsinfo.dxram.lock.PeerLockComponent;
import de.hhu.bsinfo.dxram.lock.PeerLockService;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.LogService;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupService;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.migration.MigrationService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxram.recovery.RecoveryService;
import de.hhu.bsinfo.dxram.script.ScriptEngineComponent;
import de.hhu.bsinfo.dxram.script.ScriptEngineService;
import de.hhu.bsinfo.dxram.stats.StatisticsService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import de.hhu.bsinfo.dxram.term.TerminalComponent;
import de.hhu.bsinfo.dxram.term.TerminalService;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.ethnet.NodeID;
import de.hhu.bsinfo.utils.ManifestHelper;

/**
 * Main class/entry point for any application to work with DXRAM and its services.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public class DXRAM {

    private DXRAMEngine m_engine;

    /**
     * Constructor
     */
    public DXRAM() {
        m_engine = new DXRAMEngine();
        registerComponents(m_engine);
        registerServices(m_engine);
    }

    /**
     * Returns the DXRAM engine
     *
     * @return the DXRAMEngine
     */
    protected DXRAMEngine getDXRAMEngine() {
        return m_engine;
    }

    /**
     * Initialize the instance.
     *
     * @param p_autoShutdown
     *     True to have DXRAM shut down automatically when the application quits.
     *     If false, the caller has to take care of shutting down the instance by calling shutdown when done.
     * @return True if initializing was successful, false otherwise.
     */
    public boolean initialize(final boolean p_autoShutdown) {
        boolean ret = m_engine.init();
        if (!ret) {
            return false;
        }

        printNodeInfo();
        if (p_autoShutdown) {
            Runtime.getRuntime().addShutdownHook(new ShutdownThread(this));
        }
        postInit();

        return true;
    }

    /**
     * Initialize the instance.
     *
     * @param p_configurationFile
     *     Absolute or relative path to a configuration file
     * @return True if initializing was successful, false otherwise.
     */
    public boolean initialize(final String p_configurationFile) {
        preInit();
        boolean ret = m_engine.init(p_configurationFile);
        if (ret) {
            printNodeInfo();
            postInit();
        }
        return ret;
    }

    /**
     * Initialize the instance.
     *
     * @param p_autoShutdown
     *     True to have DXRAM shut down automatically when the application quits.
     *     If false, the caller has to take care of shutting down the instance by calling shutdown when done.
     * @param p_configurationFile
     *     Absolute or relative path to a configuration file
     * @return True if initializing was successful, false otherwise.
     */
    public boolean initialize(final boolean p_autoShutdown, final String p_configurationFile) {
        preInit();
        boolean ret = initialize(p_configurationFile);
        if (ret & p_autoShutdown) {
            Runtime.getRuntime().addShutdownHook(new ShutdownThread(this));
        }
        if (ret) {
            printNodeInfo();
            postInit();
        }

        return ret;
    }

    /**
     * Get a service from DXRAM.
     *
     * @param <T>
     *     Type of service to get
     * @param p_class
     *     Class of the service to get. If one service has multiple implementations, use
     *     the common super class here.
     * @return Service requested or null if the service is not enabled/available.
     */
    public <T extends AbstractDXRAMService> T getService(final Class<T> p_class) {
        return m_engine.getService(p_class);
    }

    /**
     * Shut down DXRAM. Call this if you have not enabled auto shutdown on init.
     */
    public void shutdown() {
        preShutdown();
        m_engine.shutdown();
        postShutdown();
    }

    /**
     * Register all default DXRAM components. If you want to register further components,
     * override this method but make sure to call it using super
     *
     * @param p_engine
     *     DXRAM engine instance to register components at
     */
    protected void registerComponents(final DXRAMEngine p_engine) {
        p_engine.registerComponent(BackupComponent.class);
        p_engine.registerComponent(ZookeeperBootComponent.class);
        p_engine.registerComponent(ChunkComponent.class);
        p_engine.registerComponent(EventComponent.class);
        p_engine.registerComponent(FailureComponent.class);
        p_engine.registerComponent(PeerLockComponent.class);
        p_engine.registerComponent(LogComponent.class);
        p_engine.registerComponent(LookupComponent.class);
        p_engine.registerComponent(MemoryManagerComponent.class);
        p_engine.registerComponent(NameserviceComponent.class);
        p_engine.registerComponent(NetworkComponent.class);
        p_engine.registerComponent(NullComponent.class);
        p_engine.registerComponent(ScriptEngineComponent.class);
        p_engine.registerComponent(TerminalComponent.class);
    }

    /**
     * Register all default DXRAM services. If you want to register further services,
     * override this method but make sure to call it using super
     *
     * @param p_engine
     *     DXRAM engine instance to register services at
     */
    protected void registerServices(final DXRAMEngine p_engine) {
        p_engine.registerService(AsyncChunkService.class);
        p_engine.registerService(BootService.class);
        p_engine.registerService(ChunkMemoryService.class);
        p_engine.registerService(ChunkService.class);
        p_engine.registerService(LogService.class);
        p_engine.registerService(LoggerService.class);
        p_engine.registerService(LookupService.class);
        p_engine.registerService(MigrationService.class);
        p_engine.registerService(NameserviceService.class);
        p_engine.registerService(NetworkService.class);
        p_engine.registerService(NullService.class);
        p_engine.registerService(PeerLockService.class);
        p_engine.registerService(RecoveryService.class);
        p_engine.registerService(ScriptEngineService.class);
        p_engine.registerService(SynchronizationService.class);
        p_engine.registerService(TerminalService.class);
        p_engine.registerService(TemporaryStorageService.class);
        p_engine.registerService(StatisticsService.class);
    }

    /**
     * Stub method for any class extending this class.
     * Override this to run some tasks like initializing variables after
     * DXRAM has booted.
     */
    protected void postInit() {
        // stub
    }

    /**
     * Stub method for any class extending this class.
     * Override this to run cleanup before DXRAM shuts down.
     */
    protected void preShutdown() {
        // stub
    }

    /**
     * Stub method for any class extending this class.
     * Override this to run some tasks like initializing variables before
     * DXRAM has booted.
     */
    private void preInit() {
        // stub
    }

    /**
     * Stub method for any class extending this class.
     * Override this to run cleanup after DXRAM shuts down.
     */
    private void postShutdown() {
        // stub
    }

    /**
     * Print some information after init about our current node.
     */
    private void printNodeInfo() {
        String str = ">>> DXRAM Node <<<\n";
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
         *     Reference to DXRAM instance.
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
