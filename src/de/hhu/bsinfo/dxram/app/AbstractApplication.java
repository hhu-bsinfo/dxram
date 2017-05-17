package de.hhu.bsinfo.dxram.app;

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

    private DXRAMEngine m_dxram;

    // TODO doc
    public abstract DXRAMVersion getBuiltAgainstVersion();

    public abstract String getApplicationName();

    public abstract void main(final String[] p_args);

    public abstract void signalShutdown();

    @Override
    public void run() {
        setName(getApplicationName());

        // #if LOGGER >= INFO
        LOGGER.info("Starting '%s'...", getName());
        // #endif /* LOGGER >= INFO */

        // TODO pass parameters to application -> config file like dxram?
        main(new String[0]);

        // #if LOGGER >= INFO
        LOGGER.info("'%s' finished", getName());
        // #endif /* LOGGER >= INFO */
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

    void setEngine(final DXRAMEngine p_dxram) {
        m_dxram = p_dxram;
    }
}
