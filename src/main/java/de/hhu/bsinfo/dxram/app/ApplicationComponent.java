package de.hhu.bsinfo.dxram.app;

import java.util.Collection;
import java.util.List;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMModule;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;

/**
 * Component to run applications locally on the DXRAM instance with access to all exposed services
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
 */
@AbstractDXRAMModule.Attributes(supportsSuperpeer = false, supportsPeer = true)
@AbstractDXRAMComponent.Attributes(priorityInit = DXRAMComponentOrder.Init.APPLICATION,
        priorityShutdown = DXRAMComponentOrder.Shutdown.APPLICATION)
public class ApplicationComponent extends AbstractDXRAMComponent<ApplicationComponentConfig> {
    private ApplicationLoader m_loader;
    private ApplicationRunner m_runner;

    /**
     * Start an application
     *
     * @param p_class
     *         Fully qualified class name of application to start
     * @param p_args
     *         Arguments for application
     * @return True if starting application was successful, false on error
     */
    public boolean startApplication(final String p_class, final String[] p_args) {
        return m_runner.startApplication(p_class, p_args);
    }

    /**
     * Shutdown a running application. This triggers the shutdown signal to allow the application
     * to initiate a soft shutdown
     *
     * @param p_class
     *         Fully qualified class name of application to shut down
     */
    public void shutdownApplication(final String p_class) {
        m_runner.shutdownApplication(p_class);
    }

    /**
     * Get a list of loaded application classes (available for starting)
     *
     * @return List of application classes loaded
     */
    public List<String> getLoadedApplicationClasses() {
        return m_loader.getLoadedApplicationClasses();
    }

    /**
     * Get a list of currently running applications
     *
     * @return List of currently running applications
     */
    public List<String> getApplicationsRunning() {
        return m_runner.getApplicationsRunning();
    }

    public Collection<ApplicationProcess> getRunningProcesses() {
        return m_runner.getRunningProcesses();
    }

    /**
     * Register an application class at the loader (external). Allows registering in-code
     * application classes without having to load jars (e.g. for testing).
     *
     * @param p_class
     *         Application class to register
     */
    public void registerApplicationClass(final Class<? extends AbstractApplication> p_class) {
        m_loader.registerApplicationClass(p_class);
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {

    }

    @Override
    protected boolean initComponent(final DXRAMConfig p_config, final DXRAMJNIManager p_jniManager) {
        ApplicationComponentConfig applicationConfig = p_config.getComponentConfig(ApplicationComponent.class);

        m_loader = new ApplicationLoader(applicationConfig.getApplicationPath());
        m_runner = new ApplicationRunner(m_loader, getParentEngine().getVersion(), getParentEngine());

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        m_runner.shutdown();
        m_runner = null;
        m_loader = null;

        return true;
    }
}
