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
        LOGGER = LogManager.getFormatterLogger(getClass().getSimpleName());

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

        LOGGER.info("Initializing module...");

        boolean success = moduleInit(p_engine);

        if (!success) {
            LOGGER.error("Initializing module failed");
        } else {
            LOGGER.info("Initializing module successful");

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
            LOGGER.info("Shutting down module...");

            ret = moduleShutdown();

            if (!ret) {
                LOGGER.warn("Shutting down module failed");
            } else {
                LOGGER.info("Shutting down module successful");
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
     * Check if this class is an engine accessor i.e. breaking the rules of
     * not knowing the engine. Override this method to use that feature.
     * Do not override this if you do not know what you are doing.
     *
     * @return True if accessor, false otherwise.
     */
    protected boolean isEngineAccessor() {
        return false;
    }

    /**
     * Check if this class is a component accessor i.e. knowing other components.
     * Override this if this feature is used.
     *
     * @return True if accessor, false otherwise.
     */
    protected boolean isComponentAccessor() {
        return false;
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
     * Get the engine within the module.
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
     * Get the proxy class to access other services.
     *
     * @return This returns a valid accessor only if the class is declared a service accessor.
     */
    protected DXRAMServiceAccessor getServiceAccessor() {
        if (isServiceAccessor()) {
            return (DXRAMServiceAccessor) m_parentEngine;
        } else {
            return null;
        }
    }

    /**
     * Get the proxy class to access other components.
     *
     * @return This returns a valid accessor only if the class is declared a component accessor.
     */
    protected DXRAMComponentAccessor getComponentAccessor() {
        if (isComponentAccessor()) {
            return (DXRAMComponentAccessor) m_parentEngine;
        } else {
            return null;
        }
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
