package de.hhu.bsinfo.dxram.app;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

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
public class ApplicationService extends AbstractDXRAMService<ApplicationServiceConfig> {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ApplicationService.class.getSimpleName());

    // component dependencies
    private AbstractBootComponent m_boot;
    private ApplicationComponent m_application;

    private List<AbstractApplication> m_applications;

    /**
     * Constructor
     */
    public ApplicationService() {
        super("app", ApplicationServiceConfig.class);
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
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_application = p_componentAccessor.getComponent(ApplicationComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.Config p_config) {
        if (m_boot.getNodeRole() == NodeRole.PEER) {
            List<Class<? extends AbstractApplication>> applicationClasses = m_application.getApplicationClasses();

            m_applications = new ArrayList<>();

            // #if LOGGER >= DEBUG
            LOGGER.debug("Loading %d applications...", applicationClasses.size());
            // #endif /* LOGGER >= DEBUG */

            DXRAMVersion curVersion = getParentEngine().getVersion();

            for (Class<? extends AbstractApplication> appClass : applicationClasses) {
                // check if a configuration file for the application exists
                AbstractApplication app;
                File configFile = new File(m_application.getApplicationPath() + '/' + appClass.getSimpleName() + ".conf");

                if (configFile.exists()) {
                    app = loadFromConfiguration(appClass, configFile.getAbsolutePath());

                    // loading failed
                    if (app == null) {
                        continue;
                    }
                } else {
                    try {
                        app = appClass.newInstance();
                    } catch (IllegalAccessException | InstantiationException e) {
                        // #if LOGGER >= ERROR
                        LOGGER.error("Creating instance of application class %s failed: %s", appClass.getName(), e.getMessage());
                        // #endif /* LOGGER >= ERROR */

                        continue;
                    }

                    // generate configuration file
                    if (app.useConfigurationFile()) {
                        // #if LOGGER >= DEBUG
                        LOGGER.debug("Missing config for application '%s', creating...", app.getName());
                        // #endif /* LOGGER >= DEBUG */

                        if (!createDefaultConfiguration(app, configFile.getAbsolutePath())) {
                            continue;
                        }
                    }
                }

                if (!app.isEnabled()) {
                    // #if LOGGER >= DEBUG
                    LOGGER.debug("Application '%s' disabled", app.getApplicationName());
                    // #endif /* LOGGER >= DEBUG */

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

    /**
     * Load an existing configuration of an application
     *
     * @param p_appClass
     *         Class of the application
     * @param p_configFilePath
     *         Path to existing configuration file
     * @return Application instance with configuration values loaded or null if loading failed
     */
    private AbstractApplication loadFromConfiguration(Class<? extends AbstractApplication> p_appClass, final String p_configFilePath) {
        AbstractApplication app;

        // #if LOGGER >= INFO
        LOGGER.info("Loading configuration '%s'...", p_configFilePath);
        // #endif /* LOGGER >= INFO */

        Gson gson = ApplicationGsonContext.createGsonInstance(p_appClass);

        JsonElement element;
        try {
            element = gson.fromJson(new String(Files.readAllBytes(Paths.get(p_configFilePath))), JsonElement.class);
        } catch (final Exception e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could not load configuration '%s': %s", p_configFilePath, e.getMessage());
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        try {
            app = gson.fromJson(element, AbstractApplication.class);
        } catch (final Exception e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Loading configuration '%s' failed: %s", p_configFilePath, e.getMessage());
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        return app;
    }

    /**
     * Create a default configuration for an application
     *
     * @param p_app
     *         Application to create a default configuration for
     * @param p_configFilePath
     *         Path for configuration file
     * @return True if successful and config file was created, false on error
     */
    boolean createDefaultConfiguration(final AbstractApplication p_app, final String p_configFilePath) {
        File file = new File(p_configFilePath);
        if (file.exists()) {
            if (!file.delete()) {
                // #if LOGGER >= ERROR
                LOGGER.error("Deleting existing config file %s failed", file);
                // #endif /* LOGGER >= ERROR */
                return false;
            }
        }

        try {
            if (!file.createNewFile()) {
                // #if LOGGER >= ERROR
                LOGGER.error("Creating new config file %s failed", file);
                // #endif /* LOGGER >= ERROR */
                return false;
            }
        } catch (final IOException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Creating new config file %s failed: %s", file, e.getMessage());
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        Gson gson = ApplicationGsonContext.createGsonInstance(p_app.getClass());
        String jsonString = gson.toJson(p_app);

        try {
            PrintWriter writer = new PrintWriter(file);
            writer.print(jsonString);
            writer.close();
        } catch (final FileNotFoundException e) {
            // we can ignored this here, already checked
        }

        return true;
    }
}
