package de.hhu.bsinfo.dxram.app;

import com.google.gson.annotations.Expose;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Base class to implement and run applications on DXRAM peer nodes
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
 */
public abstract class AbstractApplication extends Thread {
    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractApplication.class.getSimpleName());

    private String m_args[] = new String[0];

    // config values
    /**
     * Name of the component (full class path)
     */
    @Expose
    private String m_class = getClass().getName();

    /**
     * True to enable the component, false to disable
     */
    @Expose
    private boolean m_enabled = true;

    /**
     * Id to set the init order. Applications (main method) are initialized from lowest to highest id
     * Init order might matter if your application depends on other dxapps to be available (e.g. to be used
     * as a library)
     */
    @Expose
    private int m_initOrderId = 0;

    private DXRAMEngine m_dxram;

    /**
     * Check if the application is enabled.
     *
     * @return True if enabled, false otherwise
     */
    public boolean isEnabled() {
        return m_enabled;
    }

    /**
     * Get the init order id of this application (call of init method)
     *
     * @return Init order id
     */
    public int getInitOrderId() {
        return m_initOrderId;
    }

    /**
     * Get the version of DXRAM the application is built against.
     * The application service will check if your application might be incompatible with
     * the current DXRAM version.
     *
     * @return DXRAMVersion this application is built against
     */
    public abstract DXRAMVersion getBuiltAgainstVersion();

    /**
     * Get the name of the application
     *
     * @return Application name
     */
    public abstract String getApplicationName();

    /**
     * Tell the ApplicationService that your application wants to use a dedicated standalone DXRAM style
     * configuration file.
     *
     * @return True if your application wants to use a configuration file, false otherwise
     */
    public abstract boolean useConfigurationFile();

    /**
     * Initialize your application. The init methods of all applications loaded are called by a single thread in the
     * order declared by m_initOrderId.
     * Override this method if required.
     */
    public void init() {
        // default stub
    }

    /**
     * The main entry point for your application.
     * Your application will run in its own thread started by the ApplicationService. If this call returns, your
     * application has terminated.
     * Note: As every main of every application is run in a separate thread, you can not make assumption about the
     * order of execution. Implement all application main's independent of each other.
     */
    public abstract void main(CommandLine p_commandLine);

    /**
     * Returns all options supported by the application.
     *
     * @return All options supported by the application.
     */
    protected List<Option> getOptions() {
        return Collections.emptyList();
    }

    /**
     * Signal by DXRAM to shut down your application.
     * When called, exit any loops, shut down further child threads and initiate clean up of resources.
     */
    public abstract void signalShutdown();

    /**
     * Get a list of external dependencies (other java libs as jars).
     * Override this method if required.
     *
     * @return List of dependencies as string array
     */
    public String[] getExternalDependencies() {
        return new String[0];
    }

    @Override
    public void run() {
        setName(getApplicationName());

        Options options = new Options();
        getOptions().forEach(options::addOption);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, m_args);
        } catch (ParseException p_e) {
            LOGGER.error("Application options could not be parsed", p_e);
            return;
        }

        LOGGER.info("Starting '%s'...", getName());

        try {
            main(cmd);
        } catch (final Exception e) {
            LOGGER.info("Exception in application", e);
        }

        LOGGER.info("'%s' finished", getName());
    }

    /**
     * Get a service from DXRAM.
     *
     * @param <T>
     *         Type of the implemented service.
     * @param p_class
     *         Class of the service to get.
     * @return DXRAM service or null if not available.
     */
    protected <T extends AbstractDXRAMService> T getService(final Class<T> p_class) {
        return m_dxram.getService(p_class);
    }

    /**
     * Check if a service is available/enabled
     *
     * @param p_class
     *         Class of the service to check
     * @param <T>
     *         Class extending DXRAMService
     * @return True if service available/enabled, false otherwise
     */
    protected <T extends AbstractDXRAMService> boolean isServiceAvailable(final Class<T> p_class) {
        return m_dxram.isServiceAvailable(p_class);
    }

    /**
     * Set the engine to allow access to services
     *
     * @param p_dxram
     *         Engine
     */
    void setEngine(final DXRAMEngine p_dxram) {
        m_dxram = p_dxram;
    }

    /**
     * Sets the application's arguments.
     *
     * @param p_args The arguments.
     */
    void setArguments(final String[] p_args) {
        if (p_args == null) {
            return;
        }

        m_args = Arrays.copyOf(p_args, p_args.length);
    }
}
