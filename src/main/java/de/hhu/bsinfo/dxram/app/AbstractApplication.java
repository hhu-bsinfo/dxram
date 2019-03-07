package de.hhu.bsinfo.dxram.app;

import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;

/**
 * Base class to implement and run applications on DXRAM peer nodes
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
 */
public abstract class AbstractApplication extends Thread {
    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractApplication.class);

    private DXRAMServiceAccessor m_dxramServiceAccessor;
    private ApplicationCallbackHandler m_callbackHandler;
    private String[] m_args;

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
     * The main entry point for your application.
     * Your application will run in its own thread started by the ApplicationService. If this call returns, your
     * application has terminated.
     * Note: As every main of every application is run in a separate thread, you can not make assumption about the
     * order of execution. Implement all application main's independent of each other.
     *
     * @param p_args
     *         Arguments passed to your application
     */
    public abstract void main(final String[] p_args);

    /**
     * Signal by DXRAM to shut down your application.
     * When called, exit any loops, shut down further child threads and initiate clean up of resources.
     */
    public abstract void signalShutdown();

    @Override
    public void run() {
        setName(getApplicationName());

        m_callbackHandler.started(this);

        try {
            main(m_args);
        } catch (final Exception e) {
            LOGGER.info("Exception in application", e);
        }

        m_callbackHandler.finished(this);
    }

    /**
     * Init the application class before starting it
     *
     * @param p_dxramServiceAccessor
     *         DXRAM service accessor
     * @param p_callbackHandler
     *         Callback handler for application state change
     * @param p_args
     *         Args for application
     */
    void init(final DXRAMServiceAccessor p_dxramServiceAccessor, final ApplicationCallbackHandler p_callbackHandler,
            final String[] p_args) {
        m_dxramServiceAccessor = p_dxramServiceAccessor;
        m_callbackHandler = p_callbackHandler;
        m_args = p_args;
    }

    /**
     * Returns this application's arguments.
     *
     * @return This application's arguments.
     */
    public String getArguments() {
        return String.join(" ", m_args);
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
        return m_dxramServiceAccessor.getService(p_class);
    }

    /**
     * Check if a service is available
     *
     * @param p_class
     *         Class of the service to check. If the service has different implementations, use the common interface
     *         or abstract class to get the registered instance.
     * @return True if available, false otherwise
     */
    protected boolean isServiceAvailable(final Class<? extends AbstractDXRAMService> p_class) {
        return m_dxramServiceAccessor.isServiceAvailable(p_class);
    }
}
