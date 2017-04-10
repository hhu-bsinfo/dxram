/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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
 * Base class for all components in DXRAM. A component serves the engine as a building block
 * providing features and functions for a specific task. Splitting tasks/concepts/functions
 * across multiple components allows introducing a clearer structure and higher flexibility
 * for the whole system. Components are allowed to depend on other components i.e. directly
 * interact with each other.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public abstract class AbstractDXRAMComponent {

    private final Logger LOGGER;

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

    private final short m_priorityInit;
    private final short m_priorityShutdown;

    private DXRAMEngine m_parentEngine;
    private boolean m_isInitialized;

    /**
     * Constructor
     *
     * @param p_priorityInit
     *     Default init priority for this component
     * @param p_priorityShutdown
     *     Default shutdown priority for this component
     */
    protected AbstractDXRAMComponent(final short p_priorityInit, final short p_priorityShutdown) {
        LOGGER = LogManager.getFormatterLogger(getClass().getSimpleName());
        m_priorityInit = p_priorityInit;
        m_priorityShutdown = p_priorityShutdown;
    }

    /**
     * Called before the component is initialized. Get all the components your own component depends on.
     *
     * @param p_componentAccessor
     *     Component accessor that provides access to other components
     */
    protected abstract void resolveComponentDependencies(DXRAMComponentAccessor p_componentAccessor);

    /**
     * Called when the component is initialized. Setup data structures, get dependent components, read settings etc.
     *
     * @param p_engineEngineSettings
     *     EngineSettings instance provided by the engine.
     * @return True if initialing was successful, false otherwise.
     */
    protected abstract boolean initComponent(DXRAMContext.EngineSettings p_engineEngineSettings);

    /**
     * Called when the component gets shut down. Cleanup your component in here.
     *
     * @return True if shutdown was successful, false otherwise.
     */
    protected abstract boolean shutdownComponent();

    /**
     * Initialize this component.
     *
     * @param p_engine
     *     Engine this component is part of (parent).
     * @return True if initializing was successful, false otherwise.
     */
    public boolean init(final DXRAMEngine p_engine) {
        boolean ret;

        m_parentEngine = p_engine;

        // #if LOGGER >= INFO
        LOGGER.info("Initializing component...");
        // #endif /* LOGGER >= INFO */

        resolveComponentDependencies(p_engine);

        try {
            ret = initComponent(m_parentEngine.getSettings());
        } catch (final Exception e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Initializing component failed", e);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        if (!ret) {
            // #if LOGGER >= ERROR
            LOGGER.error("Initializing component failed");
            // #endif /* LOGGER >= ERROR */
        } else {
            // #if LOGGER >= INFO
            LOGGER.info("Initializing component successful");
            // #endif /* LOGGER >= INFO */

            m_isInitialized = true;
        }

        return ret;
    }

    /**
     * Finish initialization of this component when all services are running.
     *
     * @return True if finishing initialization was successful, false otherwise.
     */
    public boolean finishInitComponent() {
        return m_isInitialized;
    }

    // ------------------------------------------------------------------------------

    /**
     * Shut down this component.
     *
     * @return True if shutting down was successful, false otherwise.
     */
    public boolean shutdown() {
        boolean ret = true;

        if (m_isInitialized) {
            // #if LOGGER >= INFO
            LOGGER.info("Shutting down component...");
            // #endif /* LOGGER >= INFO */
            ret = shutdownComponent();
            if (!ret) {
                // #if LOGGER >= WARN
                LOGGER.warn("Shutting down component failed");
                // #endif /* LOGGER >= WARN */
            } else {
                // #if LOGGER >= INFO
                LOGGER.info("Shutting down component successful");
                // #endif /* LOGGER >= INFO */
            }

            m_isInitialized = false;
        }

        return ret;
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
     * Get the engine within the component.
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
     * Get the name of this component.
     *
     * @return Name of this component.
     */
    String getComponentName() {
        return getClass().getSimpleName();
    }

    /**
     * Get the init priority.
     *
     * @return Init priority.
     */
    int getPriorityInit() {
        return m_priorityInit;
    }

    /**
     * Get the shutdown priority.
     *
     * @return Shutdown priority.
     */
    int getPriorityShutdown() {
        return m_priorityShutdown;
    }
}
