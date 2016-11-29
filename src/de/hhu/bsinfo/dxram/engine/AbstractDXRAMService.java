/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.engine;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Base class for all services in DXRAM. All services in DXRAM form the API for the user.
 * Furthermore, different services allow splitting functionality that can be turned on/off
 * for different applications, create a clearer structure and higher flexibility. Services
 * use components to implement their functionality. A service is not allowed to depend on
 * another service and services can not interact with each other.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 25.01.2016
 */
public abstract class AbstractDXRAMService {

    private final Logger LOGGER;

    // config values
    @Expose
    private final String m_class = getClass().getName();
    @Expose
    private final boolean m_enabled = true;

    private String m_shortName;
    private DXRAMEngine m_parentEngine;

    /**
     * Constructor
     *
     * @param p_shortName
     *     Short name of the service (used for terminal)
     */
    protected AbstractDXRAMService(final String p_shortName) {
        LOGGER = LogManager.getFormatterLogger(getClass().getSimpleName());
        m_shortName = p_shortName;
    }

    /**
     * Get the short name/identifier for this service.
     *
     * @return Identifier/name for this service.
     */
    String getShortName() {
        return m_shortName;
    }

    /**
     * Get the name of this service.
     *
     * @return Name of this service.
     */
    String getServiceName() {
        return getClass().getSimpleName();
    }

    /**
     * Check if this class is a service accessor i.e. breaking the rules of
     * not knowing other services. Override this if this feature is used.
     *
     * @return True if accessor, false otherwise.
     */
    protected boolean isServiceAccessor() {
        return false;
    }

    /**
     * Check if this class is an engine accessor i.e. breaking the rules of
     * not knowing the engine. Override this if this feature is used.
     * Do not override this if you do not know what you are doing.
     *
     * @return True if accessor, false otherwise.
     */
    protected boolean isEngineAccessor() {
        return false;
    }

    /**
     * Get the proxy class to access other services.
     *
     * @return This returns a valid accessor only if the class is declared a service accessor.
     */
    protected DXRAMServiceAccessor getServiceAccessor() {
        if (isServiceAccessor()) {
            return m_parentEngine;
        } else {
            return null;
        }
    }

    /**
     * Get the engine within the service.
     * If you don't know what you are doing, do not use this.
     * There are some internal exceptions that make this necessary (like triggering a shutdown or reboot)
     *
     * @return Returns the parent engine if allowed to do so (override isEngineAccessor), null otherwise.
     */
    protected DXRAMEngine getParentEngine() {
        if (isEngineAccessor()) {
            return m_parentEngine;
        } else {
            return null;
        }
    }

    /**
     * Start this service.
     *
     * @param p_engine
     *     Engine this service is part of (parent).
     * @return True if initializing was successful, false otherwise.
     */
    public boolean start(final DXRAMEngine p_engine) {
        boolean ret;

        m_parentEngine = p_engine;

        // #if LOGGER >= INFO
        LOGGER.info("Starting service...");
        // #endif /* LOGGER >= INFO */

        resolveComponentDependencies(p_engine);

        try {
            ret = startService(m_parentEngine.getSettings());
        } catch (final Exception e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Starting service failed", e);
            // #endif /* LOGGER >= ERROR */

            return false;
        }

        if (!ret) {
            // #if LOGGER >= ERROR
            LOGGER.error("Starting service failed");
            // #endif /* LOGGER >= ERROR */
        } else {
            // #if LOGGER >= INFO
            LOGGER.info("Starting service successful");
            // #endif /* LOGGER >= INFO */
        }

        return ret;
    }

    /**
     * Shut down this service.
     *
     * @return True if shutting down was successful, false otherwise.
     */
    public boolean shutdown() {
        boolean ret;

        // #if LOGGER >= INFO
        LOGGER.info("Shutting down service...");
        // #endif /* LOGGER >= INFO */
        ret = shutdownService();
        if (!ret) {
            // #if LOGGER >= WARN
            LOGGER.warn("Shutting down service failed");
            // #endif /* LOGGER >= WARN */
        } else {
            // #if LOGGER >= INFO
            LOGGER.info("Shutting down service successful");
            // #endif /* LOGGER >= INFO */
        }

        return ret;
    }

    /**
     * Called before the service is initialized. Get all the components your service depends on.
     *
     * @param p_componentAccessor
     *     Component accessor that provides access to the components
     */
    protected abstract void resolveComponentDependencies(DXRAMComponentAccessor p_componentAccessor);

    /**
     * Called when the service is initialized. Setup data structures, get components for operation, read settings etc.
     *
     * @param p_engineEngineSettings
     *     EngineSettings instance provided by the engine.
     * @return True if initialing was successful, false otherwise.
     */
    protected abstract boolean startService(DXRAMContext.EngineSettings p_engineEngineSettings);

    /**
     * Called when the service gets shut down. Cleanup your service in here.
     *
     * @return True if shutdown was successful, false otherwise.
     */
    protected abstract boolean shutdownService();
}
