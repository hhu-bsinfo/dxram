package de.hhu.bsinfo.dxram.engine;

import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Provides configuration values for either a component or service. Use this as a base class for all components
 * and services to add further configuration values. If there is no need for additional configuration parameters, you
 * can use this class as a minimal and default configuration when registering modules as well.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
@Accessors(prefix = "m_")
@ToString
public class DXRAMModuleConfig {
    protected final Logger LOGGER;

    /**
     * Fully qualified class name of config to allow object creation with gson
     */
    @Expose
    @Getter
    private String m_configClassName;

    /**
     * Get the class name of the module of this configuration
     */
    @Getter
    private String m_moduleClassName;

    /**
     * Default constructor for Gson
     */
    public DXRAMModuleConfig() {
        LOGGER = LogManager.getFormatterLogger(getClass());
    }

    /**
     * Constructor
     *
     * @param p_moduleName
     *         Class name of the associated module
     */
    public DXRAMModuleConfig(final String p_moduleName) {
        LOGGER = LogManager.getFormatterLogger(getClass());
        m_configClassName = getClass().getName();
        m_moduleClassName = p_moduleName;
    }

    /**
     * Constructor
     *
     * @param p_module
     *         Class of the module that uses this configuration
     */
    public DXRAMModuleConfig(final Class<?> p_module) {
        LOGGER = LogManager.getFormatterLogger(getClass());
        m_configClassName = getClass().getName();
        m_moduleClassName = p_module.getSimpleName();
    }

    /**
     * Verify the configuration values: Check limits, validate strings, ...
     *
     * @param p_config
     *         Full configuration to access other config values on dependencies
     * @return True if verification successful, false on error
     */
    protected boolean verify(final DXRAMConfig p_config) {
        return true;
    }
}
