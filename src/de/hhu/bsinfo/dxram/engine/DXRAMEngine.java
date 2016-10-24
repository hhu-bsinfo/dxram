
package de.hhu.bsinfo.dxram.engine;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import de.hhu.bsinfo.utils.logger.LogDestination;
import de.hhu.bsinfo.utils.logger.LogDestinationConsole;
import de.hhu.bsinfo.utils.logger.LogDestinationFile;
import de.hhu.bsinfo.utils.logger.Logger;

/**
 * Main class to run DXRAM with components and services.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class DXRAMEngine implements DXRAMServiceAccessor, DXRAMComponentAccessor {

	private static final String DXRAM_ENGINE_LOG_HEADER = DXRAMEngine.class.getSimpleName();

    private DXRAMComponentManager m_componentManager;
    private DXRAMServiceManager m_serviceManager;
	private DXRAMContextHandler m_contextHandler;
	
	private boolean m_isInitilized;

	private Logger m_logger;
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
	 *
	 * @param p_class Class of the component to register
	 */
	public void registerComponent(final Class<? extends AbstractDXRAMComponent> p_class) {
		m_componentManager.register(p_class);
	}

	/**
	 * Register a DXRAM service
	 *
	 * @param p_class Class of the service to register
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
				m_logger.warn(DXRAM_ENGINE_LOG_HEADER, "Service not available " + p_class);
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
			m_logger.warn(DXRAM_ENGINE_LOG_HEADER,
					"Getting component '" + p_class.getSimpleName() + "', not available.");
		}
		// #endif /* LOGGER >= WARN */

		return component;
	}

	/**
	 * Get the settings instance of the engine.
	 *
	 * @return EngineSettings instance or null if engine is not initialized.
	 */
	DXRAMContext.EngineSettings getSettings() {
		return m_contextHandler.getContext().getEngineSettings();
	}

	/**
	 * Get the logger of the engine.
	 *
	 * @return Logger instance or null if engine is not initialized.
	 */
	Logger getLogger() {
		return m_logger;
	}

	/**
	 * Initialize DXRAM without configuration. This creates a default configuration
	 * and stores it in the default configuration path
	 *
	 * @return This will always return false because it will just generate the configuration and not start DXRAM.
	 */
	public boolean init() {
		return init("");
	}

	/**
	 * Initialize DXRAM with a configuration file
	 *
	 * @param p_configurationFile Path to configuration file. If the file does not exist, a default configuration is
 *                            	  created.
	 * @return True if initialization successful, false on error or if a new configuration was generated
	 */
	public boolean init(final String p_configurationFile) {
		assert !m_isInitilized;

		final List<AbstractDXRAMComponent> list;
		final Comparator<AbstractDXRAMComponent> comp;

		if (!bootstrap(p_configurationFile)) {
			// false indicates here that a configuration files was created
			return false;
		}

		// init the short names for the services
		for (Entry<String, AbstractDXRAMService> service : m_contextHandler.getContext().getServices().entrySet()) {
			m_servicesShortName.put(service.getValue().getShortName(), service.getKey());
		}

		// #if LOGGER >= INFO
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Initializing engine...");
		// #endif /* LOGGER >= INFO */

		// resolve dependencies of all components first
		for (AbstractDXRAMComponent component : m_contextHandler.getContext().getComponents().values()) {
			component.resolveComponentDependencies(this);
		}

		// sort list by initialization priority
		list = new ArrayList<>(m_contextHandler.getContext().getComponents().values());
		comp = (p_o1, p_o2) -> (new Integer(p_o1.getPriorityInit())).compareTo(p_o2.getPriorityInit());
		Collections.sort(list, comp);

		// #if LOGGER >= INFO
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Initializing " + list.size() + " components...");
		// #endif /* LOGGER >= INFO */
		for (AbstractDXRAMComponent component : list) {
			if (!component.init(this)) {
				// #if LOGGER >= ERROR
				m_logger.error(DXRAM_ENGINE_LOG_HEADER,
						"Initializing component '" + component.getComponentName() + "' failed, aborting init.");
				// #endif /* LOGGER >= ERROR */
				return false;
			}
		}
		// #if LOGGER >= INFO
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Initializing components done.");
		//
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Starting " + m_contextHandler.getContext().getServices().size() + " services...");
		// #endif /* LOGGER >= INFO */

		for (AbstractDXRAMService service : m_contextHandler.getContext().getServices().values()) {
			if (!service.start(this)) {
				// #if LOGGER >= ERROR
				m_logger.error(DXRAM_ENGINE_LOG_HEADER,
						"Starting service '" + service.getServiceName() + "' failed, aborting init.");
				// #endif /* LOGGER >= ERROR */
				return false;
			}
		}
		// #if LOGGER >= INFO
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Starting services done.");
		//
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Initializing engine done.");
		// #endif /* LOGGER >= INFO */
		m_isInitilized = true;

		return true;
	}

	/**
	 * Shut down the engine.
	 *
	 * @return True if successful, false otherwise.
	 */
	public boolean shutdown() {
		assert m_isInitilized;

		final List<AbstractDXRAMComponent> list;
		final Comparator<AbstractDXRAMComponent> comp;

		// #if LOGGER >= INFO
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down engine...");
		// #endif /* LOGGER >= INFO */
		// #if LOGGER >= INFO
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down " + m_contextHandler.getContext().getServices().size() + " services...");
		// #endif /* LOGGER >= INFO */

		m_contextHandler.getContext().getServices().values().stream().filter(service -> !service.shutdown()).forEach(service -> {
			// #if LOGGER >= ERROR
			m_logger.error(DXRAM_ENGINE_LOG_HEADER,
					"Shutting down service '" + service.getServiceName() + "' failed.");
			// #endif /* LOGGER >= ERROR */
		});
		m_servicesShortName.clear();

		// #if LOGGER >= INFO
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down services done.");
		// #endif /* LOGGER >= INFO */

		list = new ArrayList<>(m_contextHandler.getContext().getComponents().values());

		comp = (p_o1, p_o2) -> (new Integer(p_o1.getPriorityShutdown())).compareTo(p_o2.getPriorityShutdown());

		Collections.sort(list, comp);

		// #if LOGGER >= INFO
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down " + list.size() + " components...");
		// #endif /* LOGGER >= INFO */

		list.forEach(AbstractDXRAMComponent::shutdown);

		// #if LOGGER >= INFO
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down components done.");
		//
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down engine done.");
		// #endif /* LOGGER >= INFO */

		m_logger.close();

		m_contextHandler = null;

		m_isInitilized = false;

		return true;
	}

	/**
	 * Execute bootstrapping tasks for the engine.
	 */
	private boolean bootstrap(final String p_configurationFile) {

		String config = p_configurationFile;

		m_contextHandler = new DXRAMContextHandler(m_componentManager, m_serviceManager);

		// check vm arguments for configuration override
		String configurationOverride = System.getProperty("dxram.config");
		if (configurationOverride != null) {
			config = configurationOverride;
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

		setupLogger();

		setupJNI();

		return true;
	}

	/**
	 * Setup the logger.
	 */
	private void setupLogger() {
		m_logger = new Logger();
		m_logger.setLogLevel(m_contextHandler.getContext().getEngineSettings().getLoggerLevel());
		{
			final LogDestination logDest = new LogDestinationConsole();
			m_logger.addLogDestination(logDest, m_contextHandler.getContext().getEngineSettings().getLoggerLevelConsole());
		}
		{
			final LogDestination logDest = new LogDestinationFile(m_contextHandler.getContext().getEngineSettings().getLoggerFilePath(),
					m_contextHandler.getContext().getEngineSettings().loggerFileBackUpOld());
			m_logger.addLogDestination(logDest, m_contextHandler.getContext().getEngineSettings().getLoggerFileLevel());
		}
	}

	/**
	 * Setup JNI related stuff.
	 */
	private void setupJNI() {
		m_jniManager = new DXRAMJNIManager(m_logger);
		m_jniManager.setup(m_contextHandler.getContext().getEngineSettings());
	}
}
