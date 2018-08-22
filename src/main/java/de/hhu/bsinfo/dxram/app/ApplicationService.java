package de.hhu.bsinfo.dxram.app;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;

/**
 * Service to run applications locally on the DXRAM instance with access to all exposed services
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
 */
public class ApplicationService extends AbstractDXRAMService<ApplicationServiceConfig> {
    // component dependencies
    private ApplicationComponent m_application;

//    private List<AbstractApplication> m_applications;

    private final HashMap<String, AbstractApplication> m_applications = new HashMap<>();

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
        m_application = p_componentAccessor.getComponent(ApplicationComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.Config p_config) {
        List<Class<? extends AbstractApplication>> applicationClasses = m_application.getApplicationClasses();

        LOGGER.debug("Loading %d applications...", applicationClasses.size());

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
                    LOGGER.error("Creating instance of application class %s failed: %s", appClass.getName(),
                            e.getMessage());

                    continue;
                }

                // generate configuration file
                if (app.useConfigurationFile()) {
                    LOGGER.debug("Missing config for application '%s', creating...", app.getName());

                    if (!createDefaultConfiguration(app, configFile.getAbsolutePath())) {
                        continue;
                    }
                }
            }

            if (!app.isEnabled()) {
                LOGGER.debug("Application '%s' disabled", app.getApplicationName());

                continue;
            }

            // verify if built against current version
            DXRAMVersion version = app.getBuiltAgainstVersion();

            if (version.getMajor() < curVersion.getMajor()) {
                LOGGER.error("Cannot load application '%s', major version (%s) not matching current DXRAM version " +
                                "(%s), update your application and ensure compatibility with the current DXRAM version",
                        app.getName(), version, curVersion);

                continue;
            }

            if (version.getMinor() < curVersion.getMinor()) {
                LOGGER.warn(
                        "Application '%s' built against DXRAM version %s, current version %s. Your application " +
                                "might need minor updating to ensure full compatibility with the current version.",
                        app.getName(), version, curVersion);
            }

            app.setEngine(getParentEngine());

            AbstractApplication oldApp = m_applications.putIfAbsent(app.getApplicationName(), app);

            if (oldApp != null) {
                LOGGER.warn("Application %s was already registered", oldApp.getApplicationName());
            }
        }

        return true;
    }

    @Override
    protected boolean shutdownService() {
        LOGGER.debug("Shutting down all running applications (%d)...", m_applications.size());

        List<AbstractApplication> apps = new ArrayList<>(m_applications.values());

        for (AbstractApplication app : apps) {
            app.signalShutdown();
        }

        for (AbstractApplication app : apps) {
            LOGGER.debug("Waiting for application '%s' to finish shutdown...", app.getApplicationName());

            try {
                app.join();
            } catch (final InterruptedException ignored) {

            }

            LOGGER.debug("Application '%s' shut down", app.getApplicationName());
        }

        m_applications.clear();

        return true;
    }

    @Override
    protected void engineInitFinished() {
        if (!getConfig().isAutostartEnabled()) {
            LOGGER.info("Application autostart is disabled");
            return;
        }

        List<AbstractApplication> apps = new ArrayList<>(m_applications.values());

        apps.sort(Comparator.comparingInt(AbstractApplication::getInitOrderId));

        LOGGER.info("Initializing applications...");

        // initialize sequentially to allow applications to setup stuff that might be required by other applications
        // e.g. use a dxapp as some sort of library
        for (AbstractApplication app : apps) {
            System.out.println("init order id: " + app.getInitOrderId());
            app.init();
        }

        LOGGER.info("Starting applications...");

        // start all applications
        for (AbstractApplication app : apps) {
            app.start();
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
    private AbstractApplication loadFromConfiguration(Class<? extends AbstractApplication> p_appClass,
            final String p_configFilePath) {
        AbstractApplication app;

        LOGGER.info("Loading configuration '%s'...", p_configFilePath);

        Gson gson = ApplicationGsonContext.createGsonInstance(p_appClass);

        JsonElement element;

        try {
            element = gson.fromJson(new String(Files.readAllBytes(Paths.get(p_configFilePath))), JsonElement.class);
        } catch (final Exception e) {
            LOGGER.error("Could not load configuration '%s': %s", p_configFilePath, e.getMessage());

            return null;
        }

        try {
            app = gson.fromJson(element, AbstractApplication.class);
        } catch (final Exception e) {
            LOGGER.error("Loading configuration '%s' failed: %s", p_configFilePath, e.getMessage());

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
                LOGGER.error("Deleting existing config file %s failed", file);

                return false;
            }
        }

        try {
            if (!file.createNewFile()) {
                LOGGER.error("Creating new config file %s failed", file);

                return false;
            }
        } catch (final IOException e) {
            LOGGER.error("Creating new config file %s failed: %s", file, e.getMessage());

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

    /**
     * Starts the application with the specified name.
     *
     * @param p_name The application's name.
     * @return True if the application was started successfully; false else.
     */
    public boolean startApplication(final String p_name) {
        AbstractApplication app = m_applications.get(p_name);
        if (app == null) {
            LOGGER.warn("Application %s was not registered", p_name);
            return false;
        }

        if (app.isAlive()) {
            LOGGER.warn("Application %s is already running", p_name);
            return false;
        }

        app.init();
        app.start();
        return true;
    }
}
