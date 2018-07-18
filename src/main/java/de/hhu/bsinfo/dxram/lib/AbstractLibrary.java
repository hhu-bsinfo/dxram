package de.hhu.bsinfo.dxram.lib;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;

/**
 * Base class to implement a library for DXRAM peer nodes
 *
 * @author Kai Neyenhuys, kai.neyenhuys@hhu.de 17.07.2018
 */
public abstract class AbstractLibrary {
    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractLibrary.class.getSimpleName());

    @Expose
    private final String m_class = getClass().getName();

    @Expose
    private final boolean m_enabled = true;

    private DXRAMEngine m_dxram;

    /**
     * Check if the library is enabled.
     *
     * @return True if enabled, false otherwise
     */
    public boolean isEnabled() {
        return m_enabled;
    }

    /**
     * Get the version of DXRAM the library is built against.
     * The library service will check if your library might be incompatible with
     * the current DXRAM version.
     *
     * @return DXRAMVersion this library is built against
     */
    public abstract DXRAMVersion getBuiltAgainstVersion();

    /**
     * Get the name of the library
     *
     * @return Library name
     */
    public abstract String getLibraryName();

    /**
     * Tell the LibraryService that your library wants to use a dedicated standalone DXRAM style
     * configuration file.
     *
     * @return True if your library wants to use a configuration file, false otherwise
     */
    public abstract boolean useConfigurationFile();

    /**
     * Init the library with needed services or other bootstrapping tasks
     */
    public abstract void init();

    /**
     * Signal by DXRAM to terminate your library.
     * When called, initiate clean up of resources.
     */
    public abstract void terminate();

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

