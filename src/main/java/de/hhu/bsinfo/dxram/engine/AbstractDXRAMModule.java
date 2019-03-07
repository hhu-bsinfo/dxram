package de.hhu.bsinfo.dxram.engine;

import java.lang.annotation.Annotation;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Base class with common state and functionality for services and components.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 12.11.2018
 */
public abstract class AbstractDXRAMModule<T> {
    protected final Logger LOGGER;

    private Attributes m_attributes;

    private T m_config;
    private DXRAMEngine m_parentEngine;

    private boolean m_isInitialized;

    /**
     * Constructor
     */
    AbstractDXRAMModule() {
        LOGGER = LogManager.getFormatterLogger(getClass());

        Annotation[] annotations = getClass().getAnnotations();

        for (Annotation annotation : annotations) {
            if (annotation instanceof Attributes) {
                m_attributes = (Attributes) annotation;
                break;
            }
        }
    }

    /**
     * Get the name of this module.
     *
     * @return Name of this module.
     */
    public String getName() {
        return getClass().getSimpleName();
    }

    /**
     * Get the configuration of the module
     *
     * @return Configuration of the module
     */
    public T getConfig() {
        if (m_config == null) {
            throw new IllegalStateException("Config not set for module " + getName());
        }

        return m_config;
    }

    /**
     * Initialize this module.
     *
     * @param p_engine
     *         Engine this module is part of (parent).
     * @return True if initializing was successful, false otherwise.
     */
    public boolean init(final DXRAMEngine p_engine) {
        m_parentEngine = p_engine;

        LOGGER.debug("Initializing module...");

        boolean success = moduleInit(p_engine);

        if (!success) {
            LOGGER.error("Initializing module failed");
        } else {
            LOGGER.debug("Initializing module successful");

            m_isInitialized = true;
        }

        return success;
    }

    /**
     * Shut down this module.
     *
     * @return True if shutting down was successful, false otherwise.
     */
    public boolean shutdown() {
        boolean ret = true;

        if (m_isInitialized) {
            LOGGER.debug("Shutting down module");

            ret = moduleShutdown();

            if (!ret) {
                LOGGER.warn("Shutting down module failed");
            } else {
                LOGGER.debug("Shutting down module successful");
            }

            m_parentEngine = null;
            m_isInitialized = false;
        }

        return ret;
    }

    /**
     * Set the configuration
     *
     * @param p_config
     *         Config to set
     */
    void setConfig(final T p_config) {
        m_config = p_config;
    }

    /**
     * Initialize this module.
     *
     * @param p_engine
     *         Engine this module is part of (parent).
     * @return True if initializing was successful, false otherwise.
     */
    protected abstract boolean moduleInit(final DXRAMEngine p_engine);

    /**
     * Shut down this module.
     *
     * @return True if shutting down was successful, false otherwise.
     */
    protected abstract boolean moduleShutdown();

    /**
     * Called when the engine finished initialization and all services and components are started
     */
    protected void engineInitFinished() {
        // empty
    }

    /**
     * Get the engine within the module.
     * If you don't know what you are doing, do not use this.
     * There are some internal exceptions that make this necessary
     * (e.g. accessing services from applications, jobs etc).
     *
     * @return Returns the parent engine
     */
    protected DXRAMEngine getParentEngine() {
        return m_parentEngine;
    }

    /**
     * Attributes for components
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Inherited
    public @interface Attributes {
        /**
         * True if module supports the superpeer node role, false otherwise
         */
        boolean supportsSuperpeer();

        /**
         * True if module supports the peer node role, false otherwise
         */
        boolean supportsPeer();
    }
}
