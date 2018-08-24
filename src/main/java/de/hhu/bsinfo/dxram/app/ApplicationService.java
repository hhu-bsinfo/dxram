package de.hhu.bsinfo.dxram.app;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Service to run applications locally on the DXRAM instance with access to all exposed services
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
 * @author Filip Krakowski, Filip.Krakowski@Uni-Duesseldorf.de, 22.08.2018
 */
public class ApplicationService extends AbstractDXRAMService<ApplicationServiceConfig> {
    // component dependencies
    private ApplicationComponent m_appComponent;

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
        m_appComponent = p_componentAccessor.getComponent(ApplicationComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.Config p_config) {
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
        getConfig().getAutoStartApps().forEach(clazz -> startApplication(clazz, null));
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
    @Nullable
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
     * Starts the application with the specified class.
     *
     * @param p_class The fully qualified name of the application's class.
     * @return True if the application was started successfully; false else.
     */
    public boolean startApplication(final String p_class, final String[] p_args) {
        if (p_class.isEmpty()) {
            return false;
        }

        Class<? extends AbstractApplication> appClass = m_appComponent.getApplicationClass(p_class);

        if (appClass == null) {
            LOGGER.warn("Application class %s was not found", p_class);
            return false;
        }

        AbstractApplication app;
        File configFile = new File(m_appComponent.getApplicationPath() + '/' + appClass.getSimpleName() + ".conf");

        if (configFile.exists()) {
            app = loadFromConfiguration(appClass, configFile.getAbsolutePath());
        } else {
            try {
                app = appClass.newInstance();
            } catch (IllegalAccessException | InstantiationException e) {
                LOGGER.error("Creating instance of application class %s failed: %s", appClass.getName(),
                        e.getMessage());
                return false;
            }

            // generate configuration file
            if (app.useConfigurationFile()) {
                LOGGER.debug("Missing config for application '%s', creating...", app.getName());

                if (!createDefaultConfiguration(app, configFile.getAbsolutePath())) {
                    return false;
                }
            }
        }

        if (app == null) {
            LOGGER.warn("Application %s could not be loaded", p_class);
            return false;
        }

        if (!app.isEnabled()) {
            LOGGER.debug("Application '%s' disabled", app.getApplicationName());
            return false;
        }

        // verify if built against current version
        DXRAMVersion engineVersion = getParentEngine().getVersion();
        DXRAMVersion appVersion = app.getBuiltAgainstVersion();

        if (appVersion.getMajor() < engineVersion.getMajor()) {
            LOGGER.error("Cannot load application '%s', major version (%s) not matching current DXRAM version " +
                            "(%s), update your application and ensure compatibility with the current DXRAM version",
                    app.getName(), appVersion, engineVersion);

            return false;
        }

        if (appVersion.getMinor() < engineVersion.getMinor()) {
            LOGGER.warn(
                    "Application '%s' built against DXRAM version %s, current version %s. Your application " +
                            "might need minor updating to ensure full compatibility with the current version.",
                    app.getName(), appVersion, engineVersion);
        }

        app.setEngine(getParentEngine());
        app.setArguments(p_args);
        app.init();
        app.start();
        return true;
    }
}
