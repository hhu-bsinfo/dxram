package de.hhu.bsinfo.dxram.app;

import java.util.Comparator;
import java.util.List;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Service to run applications locally on the DXRAM instance with access to all exposed services
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
 * @author Filip Krakowski, Filip.Krakowski@Uni-Duesseldorf.de, 22.08.2018
 */
public class ApplicationService extends AbstractDXRAMService<ApplicationServiceConfig> {
    // component dependencies
    private ApplicationComponent m_appComponent;

    /**
     * Constructor
     */
    public ApplicationService() {
        super("app", ApplicationServiceConfig.class);
    }

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
        return m_appComponent.startApplication(p_class, p_args);
    }

    /**
     * Shutdown a running application. This triggers the shutdown signal to allow the application
     * to initiate a soft shutdown
     *
     * @param p_class
     *         Fully qualified class name of application to shut down
     */
    public void shutdownApplication(final String p_class) {
        m_appComponent.shutdownApplication(p_class);
    }

    /**
     * Get a list of loaded application classes (available for starting)
     *
     * @return List of application classes loaded
     */
    public List<String> getLoadedApplicationClasses() {
        return m_appComponent.getLoadedApplicationClasses();
    }

    /**
     * Get a list of currently running applications
     *
     * @return List of currently running applications
     */
    public List<String> geApplicationsRunning() {
        return m_appComponent.getApplicationsRunning();
    }

    @Override
    protected boolean supportsSuperpeer() {
        return false;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_appComponent = p_componentAccessor.getComponent(ApplicationComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.Config p_config) {
        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }

    @Override
    protected void engineInitFinished() {
        List<ApplicationServiceConfig.ApplicationEntry> list = getConfig().getAutoStart();
        list.sort(Comparator.comparingInt(ApplicationServiceConfig.ApplicationEntry::getStartOrderId));
        getConfig().getAutoStart().forEach(entry -> startApplication(entry.getClassName(), entry.getArgs()));
    }

    @Override
    protected boolean isEngineAccessor() {
        // access the engine to hook it to the applications
        return true;
    }
}
