package de.hhu.bsinfo.dxram.app;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.util.NodeRole;

/**
 * Service to run applications locally on the DXRAM instance with access to all exposed services
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
 */
public class ApplicationService extends AbstractDXRAMService {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ApplicationService.class.getSimpleName());

    // component dependencies
    private AbstractBootComponent m_boot;
    private ApplicationComponent m_application;

    private List<AbstractApplication> m_applications;

    /**
     * Constructor
     */
    public ApplicationService() {
        super("app");
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_application = p_componentAccessor.getComponent(ApplicationComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        if (m_boot.getNodeRole() == NodeRole.PEER) {
            List<Class<? extends AbstractApplication>> applicationClasses = m_application.getApplicationClasses();

            m_applications = new ArrayList<>();

            // #if LOGGER >= DEBUG
            LOGGER.debug("Loading %d applications...", applicationClasses.size());
            // #endif /* LOGGER >= DEBUG */

            DXRAMVersion curVersion = getParentEngine().getVersion();

            for (Class<? extends AbstractApplication> appClass : applicationClasses) {
                AbstractApplication app;

                try {
                    app = appClass.newInstance();
                } catch (IllegalAccessException | InstantiationException e) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Creating instance of application class %s failed: %s", appClass.getName(), e.getMessage());
                    // #endif /* LOGGER >= ERROR */

                    continue;
                }

                // verify if built against current version
                DXRAMVersion version = app.getBuiltAgainstVersion();

                if (version.getMajor() < curVersion.getMajor()) {
                    // #if LOGGER >= ERROR
                    LOGGER.error(
                            "Cannot load application '%s', major version (%s) not matching current DXRAM version (%s), update your application and ensure " +
                                    "compatibility with the current DXRAM version", app.getName(), version, curVersion);
                    // #endif /* LOGGER >= ERROR */
                    continue;
                }

                if (version.getMinor() < curVersion.getMinor()) {
                    // #if LOGGER >= WARN
                    LOGGER.warn(
                            "Application '%s' built against DXRAM version %s, current version %s. Your application might need minor updating to ensure full " +
                                    "compatibility with the current version.", app.getName(), version, curVersion);
                    // #endif /* LOGGER >= WARN */
                }

                app.setEngine(getParentEngine());

                m_applications.add(app);
            }
        }

        return true;
    }

    @Override
    protected boolean shutdownService() {
        if (m_boot.getNodeRole() == NodeRole.PEER) {
            // #if LOGGER >= DEBUG
            LOGGER.debug("Shutting down all running applications (%d)...", m_applications.size());
            // #endif /* LOGGER >= DEBUG */

            for (AbstractApplication app : m_applications) {
                app.signalShutdown();
            }

            for (AbstractApplication app : m_applications) {
                // #if LOGGER >= DEBUG
                LOGGER.debug("Waiting for application '%s' to finish shutdown...", app.getApplicationName());
                // #endif /* LOGGER >= DEBUG */

                try {
                    app.join();
                } catch (final InterruptedException ignored) {

                }

                // #if LOGGER >= INFO
                LOGGER.debug("Application '%s' shut down", app.getApplicationName());
                // #endif /* LOGGER >= INFO */
            }

            m_applications.clear();
        }

        return true;
    }

    @Override
    protected void engineInitFinished() {
        if (m_boot.getNodeRole() == NodeRole.PEER) {
            // start all applications
            for (AbstractApplication app : m_applications) {
                app.start();
            }
        }
    }

    @Override
    protected boolean isEngineAccessor() {
        // access the engine to hook it to the applications
        return true;
    }
}
