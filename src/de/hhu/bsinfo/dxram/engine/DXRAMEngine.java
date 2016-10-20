
package de.hhu.bsinfo.dxram.engine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import de.hhu.bsinfo.utils.logger.LogDestination;
import de.hhu.bsinfo.utils.logger.LogDestinationConsole;
import de.hhu.bsinfo.utils.logger.LogDestinationFile;
import de.hhu.bsinfo.utils.logger.Logger;

/**
 * Main class to run DXRAM with components and services.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */

public class DXRAMEngine implements DXRAMServiceAccessor {

	private static final String DXRAM_ENGINE_LOG_HEADER = DXRAMEngine.class.getSimpleName();
	private static final String DXRAM_CONFIG_FILE_PATH = "config/dxram.json";

	private DXRAMContext m_context;

	private boolean m_isInitilized;

	private Logger m_logger;
	private DXRAMJNIManager m_jniManager;

	private Map<String, String> m_servicesShortName = new HashMap<>();

	/**
	 * Constructor
	 */
	public DXRAMEngine() {

	}

	@Override
	public <T extends AbstractDXRAMService> T getService(final Class<T> p_class) {
		T service = null;

		if (m_isInitilized) {
			AbstractDXRAMService tmpService = m_context.getServices().get(p_class.getName());
			if (tmpService == null) {
				// check for any kind of instance of the specified class
				// we might have another interface/abstract class between the
				// class we request and an instance we could serve
				for (Entry<String, AbstractDXRAMService> entry : m_context.getServices().entrySet()) {
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
			return m_context.getServices().get(m_servicesShortName.get(p_shortName));
		}

		return null;
	}

	@Override
	public List<String> getServiceShortNames() {
		return new ArrayList<>(m_servicesShortName.keySet());
	}

	/**
	 * Get a component from the engine.
	 *
	 * @param <T>     Type of the component class.
	 * @param p_class Class of the component to get. If the component has different implementations, use the common
	 *                interface
	 *                or abstract class to get the registered instance.
	 * @return Reference to the component if available and enabled, null otherwise or if the engine is not
	 * initialized.
	 */
	<T extends AbstractDXRAMComponent> T getComponent(final Class<T> p_class) {
		T component = null;

		AbstractDXRAMComponent tmpComponent = m_context.getComponents().get(p_class.getName());
		if (tmpComponent == null) {
			// check for any kind of instance of the specified class
			// we might have another interface/abstract class between the
			// class we request and an instance we could serve
			for (Entry<String, AbstractDXRAMComponent> entry : m_context.getComponents().entrySet()) {
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
		return m_context.getEngineSettings();
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
		for (Entry<String, AbstractDXRAMService> service : m_context.getServices().entrySet()) {
			m_servicesShortName.put(service.getValue().getShortName(), service.getKey());
		}

		// #if LOGGER >= INFO
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Initializing engine...");
		// #endif /* LOGGER >= INFO */

		// sort list by initialization priority
		list = new ArrayList<>(m_context.getComponents().values());
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
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Starting " + m_context.getServices().size() + " services...");
		// #endif /* LOGGER >= INFO */

		for (AbstractDXRAMService service : m_context.getServices().values()) {
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
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down " + m_context.getServices().size() + " services...");
		// #endif /* LOGGER >= INFO */

		m_context.getServices().values().stream().filter(service -> !service.shutdown()).forEach(service -> {
			// #if LOGGER >= ERROR
			m_logger.error(DXRAM_ENGINE_LOG_HEADER,
					"Shutting down service '" + service.getServiceName() + "' failed.");
			// #endif /* LOGGER >= ERROR */
		});
		m_servicesShortName.clear();

		// #if LOGGER >= INFO
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down services done.");
		// #endif /* LOGGER >= INFO */

		list = new ArrayList<>(m_context.getComponents().values());

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

		m_context = null;

		m_isInitilized = false;

		return true;
	}

	/**
	 * Execute bootstrapping tasks for the engine.
	 */
	private boolean bootstrap(final String p_configurationFile) {

		String config = p_configurationFile;

		m_context = new DXRAMContext();

		// check vm arguments for configuration override
		String configurationOverride = System.getProperty("dxram.config");
		if (configurationOverride != null) {
			config = configurationOverride;
		}

		// check if a config needs to be created
		if (config.isEmpty() || !(new File(config)).exists()) {
			createDefaultConfiguration(config);
			return false;
		}

		// load existing configuration
		if (!loadConfiguration(config)) {
			return false;
		}

		setupLogger();

		setupJNI();

		return true;
	}

	/**
	 * Create a default configuration file
	 *
	 * @param p_configFilePath Path for configuration file
	 * @return True if creating config file successful, false otherwise
	 */
	private boolean createDefaultConfiguration(final String p_configFilePath) {

		System.out.println("No valid configuration found or specified via vm argument -Ddxram.config, "
				+ "creating default configuration '" + p_configFilePath + "'...");

		String configFilePath;

		if (p_configFilePath.isEmpty()) {
			configFilePath = DXRAM_CONFIG_FILE_PATH;
		} else {
			configFilePath = p_configFilePath;
		}

		File file = new File(configFilePath);
		if (file.exists()) {
			if (!file.delete()) {
				System.out.println("Deleting existing config file " + file + " failed");
				return false;
			}
		}

		try {
			if (!file.createNewFile()) {
				System.out.println("Creating new config file " + file + " failed");
				return false;
			}
		} catch (final IOException e) {
			System.out.println("Creating new config file " + file + " failed: " + e.getMessage());
			return false;
		}

		// create default components and services
		DXRAMComponentManager.registerDefault();
		DXRAMServiceManager.registerDefault();

		m_context.fillDefaultComponents();
		m_context.fillDefaultServices();

		Gson gson = DXRAMGsonContext.createGsonInstance();
		String jsonString = gson.toJson(m_context);

		try {
			PrintWriter writer = new PrintWriter(file);
			writer.print(jsonString);
			writer.close();
		} catch (final FileNotFoundException e) {
			// we can ignored this here, already checked that
		}

		return true;
	}

	/**
	 * Load an existing configuration
	 *
	 * @param p_configFilePath Path to existing configuration file
	 * @return True if loading successful, false on error
	 */
	private boolean loadConfiguration(final String p_configFilePath) {

		System.out.println("Loading configuration '" + p_configFilePath + "'...");

		Gson gson = DXRAMGsonContext.createGsonInstance();

		JsonElement element;
		try {
			element = gson.fromJson(new String(Files.readAllBytes(Paths.get(p_configFilePath))), JsonElement.class);
		} catch (final IOException e) {
			System.out.println("Could not load configuration '" + p_configFilePath + "': " + e.getMessage());
			return false;
		}

		overrideConfigurationWithVMArguments(element.getAsJsonObject());

		m_context = gson.fromJson(element, DXRAMContext.class);

		return true;
	}

	/**
	 * Override current configuration with further values provided via VM arguments
	 *
	 * @param p_object Root object of JSON configuration tree
	 */
	private void overrideConfigurationWithVMArguments(final JsonObject p_object) {

		Properties props = System.getProperties();
		Enumeration e = props.propertyNames();

		while (e.hasMoreElements()) {

			String key = (String) e.nextElement();
			if (key.startsWith("dxram.") && !key.equals("dxram.config")) {

				String[] tokens = key.split("\\.");

				JsonObject parent = p_object;
				JsonObject child = null;
				// skip dxram token
				for (int i = 1; i < tokens.length; i++) {

					JsonElement elem = parent.get(tokens[i]);

					// if first element is already invalid
					if (elem == null) {
						child = null;
						break;
					}

					if (elem.isJsonObject()) {
						child = elem.getAsJsonObject();
					} else if (i + 1 == tokens.length) {
						break;
					}

					if (child == null) {
						break;
					}

					parent = child;
				}

				if (child == null) {
					System.out.println("Invalid vm argument '" + key + "'");
					continue;
				}

				String propertyKey = props.getProperty(key);

				// try to determine type, not a very nice way =/
				if (propertyKey.matches(
						"^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
								+ "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
								+ "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
								+ "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$")) {
					// ip address
					parent.addProperty(tokens[tokens.length - 1], propertyKey);
				} else if (propertyKey.matches("[-+]?\\d*\\.?\\d+")) {
					// numeric
					parent.addProperty(tokens[tokens.length - 1], Long.parseLong(propertyKey));
				} else {
					// string
					parent.addProperty(tokens[tokens.length - 1], propertyKey);
				}
			}
		}
	}

	/**
	 * Setup the logger.
	 */
	private void setupLogger() {
		m_logger = new Logger();
		m_logger.setLogLevel(m_context.getEngineSettings().getLoggerLevel());
		{
			final LogDestination logDest = new LogDestinationConsole();
			m_logger.addLogDestination(logDest, m_context.getEngineSettings().getLoggerLevelConsole());
		}
		{
			final LogDestination logDest = new LogDestinationFile(m_context.getEngineSettings().getLoggerFilePath(),
					m_context.getEngineSettings().loggerFileBackUpOld());
			m_logger.addLogDestination(logDest, m_context.getEngineSettings().getLoggerFileLevel());
		}
	}

	/**
	 * Setup JNI related stuff.
	 */
	private void setupJNI() {
		m_jniManager = new DXRAMJNIManager(m_logger);
		m_jniManager.setup(m_context.getEngineSettings());
	}
}
