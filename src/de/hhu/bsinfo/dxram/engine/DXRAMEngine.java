
package de.hhu.bsinfo.dxram.engine;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Main class to run DXRAM with components and services.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public class DXRAMEngine implements DXRAMServiceAccessor, DXRAMComponentAccessor {

    private static final Logger LOGGER = LogManager.getFormatterLogger(DXRAMEngine.class.getSimpleName());

    private DXRAMComponentManager m_componentManager;
    private DXRAMServiceManager m_serviceManager;
    private DXRAMContextHandler m_contextHandler;

    private boolean m_isInitilized;

    private DXRAMJNIManager m_jniManager;

    private Map<String, String> m_servicesShortName = new HashMap<>();

    /**
     * Constructor
     */
    public DXRAMEngine() {
        m_componentManager = new DXRAMComponentManager();
        m_serviceManager = new DXRAMServiceManager();
    }

    /**
     * Register a DXRAM component
     * @param p_class
     *            Class of the component to register
     */
    public void registerComponent(final Class<? extends AbstractDXRAMComponent> p_class) {
        m_componentManager.register(p_class);
    }

    /**
     * Register a DXRAM service
     * @param p_class
     *            Class of the service to register
     */
    public void registerService(final Class<? extends AbstractDXRAMService> p_class) {
        m_serviceManager.register(p_class);
    }

    @Override
    public <T extends AbstractDXRAMService> T getService(final Class<T> p_class) {
        T service = null;

        if (m_isInitilized) {
            AbstractDXRAMService tmpService = m_contextHandler.getContext().getServices().get(p_class.getSimpleName());
            if (tmpService == null) {
                // check for any kind of instance of the specified class
                // we might have another interface/abstract class between the
                // class we request and an instance we could serve
                for (Entry<String, AbstractDXRAMService> entry : m_contextHandler.getContext().getServices().entrySet()) {
                    tmpService = entry.getValue();
                    if (p_class.isInstance(tmpService)) {
                        break;
                    }

                    tmpService = null;
                }
            }

            if (tmpService != null && p_class.isInstance(tmpService)) {
                service = p_class.cast(tmpService);
            }

            // #if LOGGER >= WARN
            if (service == null) {
                LOGGER.warn("Service not available %s", p_class);
            }
            // #endif /* LOGGER >= WARN */
        }

        return service;
    }

    @Override
    public AbstractDXRAMService getService(final String p_shortName) {
        if (m_isInitilized) {
            return m_contextHandler.getContext().getServices().get(m_servicesShortName.get(p_shortName));
        }

        return null;
    }

    @Override
    public List<String> getServiceShortNames() {
        return new ArrayList<>(m_servicesShortName.keySet());
    }

    @Override
    public <T extends AbstractDXRAMComponent> T getComponent(final Class<T> p_class) {
        T component = null;

        AbstractDXRAMComponent tmpComponent = m_contextHandler.getContext().getComponents().get(p_class.getSimpleName());
        if (tmpComponent == null) {
            // check for any kind of instance of the specified class
            // we might have another interface/abstract class between the
            // class we request and an instance we could serve
            for (Entry<String, AbstractDXRAMComponent> entry : m_contextHandler.getContext().getComponents().entrySet()) {
                tmpComponent = entry.getValue();
                if (p_class.isInstance(tmpComponent)) {
                    break;
                }

                tmpComponent = null;
            }
        }

        if (tmpComponent != null && p_class.isInstance(tmpComponent)) {
            component = p_class.cast(tmpComponent);
        }

        // #if LOGGER >= WARN
        if (component == null) {
            LOGGER.warn("Getting component '%s', not available", p_class.getSimpleName());
        }
        // #endif /* LOGGER >= WARN */

        return component;
    }

    /**
     * Get the settings instance of the engine.
     * @return EngineSettings instance or null if engine is not initialized.
     */
    DXRAMContext.EngineSettings getSettings() {
        return m_contextHandler.getContext().getEngineSettings();
    }

    /**
     * Initialize DXRAM without configuration. This creates a default configuration
     * and stores it in the default configuration path
     * @return This will always return false because it will just generate the configuration and not start DXRAM.
     */
    public boolean init() {
        return init("");
    }

    /**
     * Initialize DXRAM with a configuration file
     * @param p_configurationFile
     *            Path to configuration file. If the file does not exist, a default configuration is
     *            created.
     * @return True if initialization successful, false on error or if a new configuration was generated
     */
    public boolean init(final String p_configurationFile) {
        assert !m_isInitilized;

        final List<AbstractDXRAMComponent> list;
        final Comparator<AbstractDXRAMComponent> comp;

        // #if LOGGER >= INFO
        LOGGER.info("Initializing engine with configuration '%s'", p_configurationFile);
        // #endif /* LOGGER >= INFO */

        if (!bootstrap(p_configurationFile)) {
            // false indicates here that a configuration files was created
            return false;
        }

        // init the short names for the services
        for (Entry<String, AbstractDXRAMService> service : m_contextHandler.getContext().getServices().entrySet()) {
            m_servicesShortName.put(service.getValue().getShortName(), service.getKey());
        }

        list = new ArrayList<>(m_contextHandler.getContext().getComponents().values());

        // check list for null objects -> invalid component in list
        for (AbstractDXRAMComponent c : list) {
            if (c == null) {
                LOGGER.fatal("Found null object in component list, most likely due to invalid configuration entry");
                return false;
            }
        }

        // sort list by initialization priority
        comp = (p_o1, p_o2) -> (new Integer(p_o1.getPriorityInit())).compareTo(p_o2.getPriorityInit());
        Collections.sort(list, comp);

        // #if LOGGER >= INFO
        LOGGER.info("Initializing %d components...", list.size());
        // #endif /* LOGGER >= INFO */
        for (AbstractDXRAMComponent component : list) {
            if (!component.init(this)) {
                // #if LOGGER >= ERROR
                LOGGER.error("Initializing component '%s' failed, aborting init", component.getComponentName());
                // #endif /* LOGGER >= ERROR */
                return false;
            }
        }
        // #if LOGGER >= INFO
        LOGGER.info("Initializing components done");
        //
        LOGGER.info("Starting %d services...", m_contextHandler.getContext().getServices().size());
        // #endif /* LOGGER >= INFO */

        for (AbstractDXRAMService service : m_contextHandler.getContext().getServices().values()) {
            // check for null -> invalid service
            if (service == null) {
                LOGGER.fatal("Found null object in service list, most likely due to invalid configuration entry");
                return false;
            }

            if (!service.start(this)) {
                // #if LOGGER >= ERROR
                LOGGER.error("Starting service '%s' failed, aborting init", service.getServiceName());
                // #endif /* LOGGER >= ERROR */
                return false;
            }
        }
        // #if LOGGER >= INFO
        LOGGER.info("Starting services done");
        //
        LOGGER.info("Initializing engine done");
        // #endif /* LOGGER >= INFO */
        m_isInitilized = true;

        return true;
    }

    /**
     * Shut down the engine.
     * @return True if successful, false otherwise.
     */
    public boolean shutdown() {
        assert m_isInitilized;

        final List<AbstractDXRAMComponent> list;
        final Comparator<AbstractDXRAMComponent> comp;

        // #if LOGGER >= INFO
        LOGGER.info("Shutting down engine...");
        // #endif /* LOGGER >= INFO */
        // #if LOGGER >= INFO
        LOGGER.info("Shutting down %d services...", m_contextHandler.getContext().getServices().size());
        // #endif /* LOGGER >= INFO */

        m_contextHandler.getContext().getServices().values().stream().filter(service -> !service.shutdown()).forEach(service -> {
            // #if LOGGER >= ERROR
            LOGGER.error("Shutting down service '%s' failed.", service.getServiceName());
            // #endif /* LOGGER >= ERROR */
        });
        m_servicesShortName.clear();

        // #if LOGGER >= INFO
        LOGGER.info("Shutting down services done");
        // #endif /* LOGGER >= INFO */

        list = new ArrayList<>(m_contextHandler.getContext().getComponents().values());

        comp = (p_o1, p_o2) -> (new Integer(p_o1.getPriorityShutdown())).compareTo(p_o2.getPriorityShutdown());

        Collections.sort(list, comp);

        // #if LOGGER >= INFO
        LOGGER.info("Shutting down %d components...", list.size());
        // #endif /* LOGGER >= INFO */

        list.forEach(AbstractDXRAMComponent::shutdown);

        // #if LOGGER >= INFO
        LOGGER.info("Shutting down components done");
        // #endif /* LOGGER >= INFO */
        // #endif /* LOGGER >= INFO */
        LOGGER.info("Shutting down engine done");
        // #endif /* LOGGER >= INFO */

        m_contextHandler = null;

        m_isInitilized = false;

        return true;
    }

    /**
     * Execute bootstrapping tasks for the engine.
     * @param p_configurationFile
     *            Configuration file to use
     */
    private boolean bootstrap(final String p_configurationFile) {
        String config = p_configurationFile;

        m_contextHandler = new DXRAMContextHandler(m_componentManager, m_serviceManager);

        // check vm arguments for configuration override
        String configurationOverride = System.getProperty("dxram.config");
        if (configurationOverride != null) {
            config = configurationOverride;
            LOGGER.info("Configuration override by vm argument: %s", config);
        }

        // check if a config needs to be created
        if (config.isEmpty() || !(new File(config)).exists()) {
            m_contextHandler.createDefaultConfiguration(config);
            return false;
        }

        // load existing configuration
        if (!m_contextHandler.loadConfiguration(config)) {
            return false;
        }

        setupJNI();

        return true;
    }

    /**
     * Setup JNI related stuff.
     */
    private void setupJNI() {
        m_jniManager = new DXRAMJNIManager();
        m_jniManager.setup(m_contextHandler.getContext().getEngineSettings());
    }
}
