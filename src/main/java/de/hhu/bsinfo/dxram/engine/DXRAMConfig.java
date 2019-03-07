package de.hhu.bsinfo.dxram.engine;

import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Root configuration class for DXRAM engine, components and services
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 20.10.2016
 */
@ToString
public class DXRAMConfig {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXRAMConfig.class);

    /**
     * Engine specific settings
     */
    @Expose
    DXRAMEngineConfig m_engineConfig = new DXRAMEngineConfig();

    /**
     * Component configurations
     */
    @Expose
    Map<String, DXRAMModuleConfig> m_componentConfigs = new HashMap<>();

    /**
     * Service configurations
     */
    @Expose
    Map<String, DXRAMModuleConfig> m_serviceConfigs = new HashMap<>();

    /**
     * Constructor
     *
     * @param p_componentConfigs Map of (default) component configuration objects
     * @param p_serviceConfigs Map of (default) service configuration objects
     */
    DXRAMConfig(final Map<String, DXRAMModuleConfig> p_componentConfigs,
            final Map<String, DXRAMModuleConfig> p_serviceConfigs) {
        m_componentConfigs = p_componentConfigs;
        m_serviceConfigs = p_serviceConfigs;
    }

    /**
     * Constructor for Gson
     */
    @SuppressWarnings("unused")
    DXRAMConfig() {

    }

    /**
     * Get the engine configuration
     */
    public DXRAMEngineConfig getEngineConfig() {
        return m_engineConfig;
    }

    /**
     * Get the configuration of a specific component
     *
     * @param p_class
     *         Class of the component configuration to get
     * @return Component configuration class
     */
    @SuppressWarnings("unchecked")
    public <T extends DXRAMModuleConfig> T getComponentConfig(final Class<? extends AbstractDXRAMComponent> p_class) {
        DXRAMModuleConfig conf = m_componentConfigs.get(p_class.getSimpleName());

        return (T) conf;
    }

    /**
     * Get the configuration of a specific service
     *
     * @param p_class
     *         Class of the service configuration to get
     * @return Service configuration class
     */
    @SuppressWarnings("unchecked")
    public <T extends DXRAMModuleConfig> T getServiceConfig(final Class<? extends AbstractDXRAMService> p_class) {
        DXRAMModuleConfig conf = m_serviceConfigs.get(p_class.getSimpleName());

        return (T) conf;
    }

    /**
     * Run configuration value verification on all component configurations
     *
     * @return True if verification successful, false on failure
     */
    boolean verifyConfigurationValuesComponents() {
        for (DXRAMModuleConfig config : m_componentConfigs.values()) {
            LOGGER.debug("Verifying component configuration values of %s...", config.getModuleClassName());

            if (!config.verify(this)) {
                LOGGER.error("Verifying component configuration values failed (%s)", config.getModuleClassName());

                return false;
            }
        }

        return true;
    }

    /**
     * Run configuration value verification on all service configurations
     *
     * @return True if verification successful, false on failure
     */
    boolean verifyConfigurationValuesServices() {
        for (DXRAMModuleConfig config : m_serviceConfigs.values()) {
            LOGGER.debug("Verifying service configuration values of %s...", config.getModuleClassName());

            if (!config.verify(this)) {
                LOGGER.error("Verifying service configuration values failed (%s)", config.getModuleClassName());

                return false;
            }
        }

        return true;
    }
}
