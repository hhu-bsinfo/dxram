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
    Map<String, ModuleConfig> m_moduleConfigs = new HashMap<>();

    /**
     * Constructor
     *
     * @param p_componentConfigs Map of (default) component configuration objects
     * @param p_serviceConfigs Map of (default) service configuration objects
     */
    DXRAMConfig(final Map<String, ModuleConfig> p_moduleConfigs) {
        m_moduleConfigs = p_moduleConfigs;
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
    public <T extends ModuleConfig> T getComponentConfig(final Class<? extends Component> p_class) {
        ModuleConfig conf = m_moduleConfigs.get(p_class.getSimpleName());

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
    public <T extends ModuleConfig> T getServiceConfig(final Class<? extends Service> p_class) {
        ModuleConfig conf = m_moduleConfigs.get(p_class.getSimpleName());

        return (T) conf;
    }

    /**
     * Run configuration value verification on all modules
     *
     * @return True if verification successful, false on failure
     */
    boolean isValid() {
        for (ModuleConfig config : m_moduleConfigs.values()) {
            LOGGER.debug("Verifying component configuration values of %s...", config.getModuleClassName());

            if (!config.verify(this)) {
                LOGGER.error("Verifying component configuration values failed (%s)", config.getModuleClassName());

                return false;
            }
        }

        return true;
    }
}
