package de.hhu.bsinfo.dxram.app;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;

/**
 * Base class to implement and run applications on DXRAM peer nodes
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
 */
public abstract class AbstractApplication extends Thread {
    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractApplication.class.getSimpleName());

    // config values
    /**
     * Name of the component (full class path)
     */
    @Expose
    private final String m_class = getClass().getName();
    /**
     * True to enable the component, false to disable
     */
    @Expose
    private final boolean m_enabled = true;

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
     * Tell the ApplicationService that your application wants to use a dedicated standalone DXRAM style configuration file.
     *
     * @return True if your application wants to use a configuration file, false otherwise
     */
    public abstract boolean useConfigurationFile();

    /**
     * The main entry point for your application.
     * Your application will run in its own thread started by the ApplicationService. If this call returns, your application has terminated.
     */
    public abstract void main();

    /**
     * Signal by DXRAM to shut down your application.
     * When called, exit any loops, shut down further child threads and initiate clean up of resources.
     */
    public abstract void signalShutdown();

    @Override
    public void run() {
        setName(getApplicationName());


        LOGGER.info("Starting '%s'...", getName());


        try {
            main();
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

    void setEngine(final DXRAMEngine p_dxram) {
        m_dxram = p_dxram;
    }
}
