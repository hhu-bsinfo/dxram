
package de.hhu.bsinfo.dxram.engine;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.conf.Configuration;
import de.hhu.bsinfo.utils.conf.ConfigurationException;
import de.hhu.bsinfo.utils.conf.ConfigurationParser;
import de.hhu.bsinfo.utils.conf.ConfigurationXMLLoader;
import de.hhu.bsinfo.utils.conf.ConfigurationXMLLoaderFile;
import de.hhu.bsinfo.utils.conf.ConfigurationXMLParser;
import de.hhu.bsinfo.utils.log.LogDestination;
import de.hhu.bsinfo.utils.log.LogDestinationConsole;
import de.hhu.bsinfo.utils.log.LogDestinationFile;
import de.hhu.bsinfo.utils.log.LogLevel;
import de.hhu.bsinfo.utils.log.Logger;

/**
 * Main class to run DXRAM with components and services.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */

public class DXRAMEngine implements DXRAMServiceAccessor {

	private static final String DXRAM_ENGINE_LOG_HEADER = DXRAMEngine.class.getSimpleName();

	private boolean m_isInitilized;

	private Configuration m_configuration;
	private Settings m_settings;
	private Logger m_logger;
	private DXRAMJNIManager m_jniManager;

	private HashMap<String, AbstractDXRAMComponent> m_components = new HashMap<String, AbstractDXRAMComponent>();
	private HashMap<String, AbstractDXRAMService> m_services = new HashMap<String, AbstractDXRAMService>();

	/**
	 * Constructor
	 */
	public DXRAMEngine() {

	}

	/**
	 * Get a service from the engine.
	 *
	 * @param p_class Class of the service to get. If the service has different implementations, use the common
	 *                interface
	 *                or abstract class to get the registered instance.
	 * @return Reference to the service if available and enabled, null otherwise or if the engine is not
	 * initialized.
	 */
	@Override
	public <T extends AbstractDXRAMService> T getService(final Class<T> p_class) {
		T service = null;

		if (m_isInitilized) {
			AbstractDXRAMService tmpService = m_services.get(p_class.getName());
			if (tmpService == null) {
				// check for any kind of instance of the specified class
				// we might have another interface/abstract class between the
				// class we request and an instance we could serve
				for (Entry<String, AbstractDXRAMService> entry : m_services.entrySet()) {
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

			if (service == null) {
				m_logger.warn(DXRAM_ENGINE_LOG_HEADER, "Service not available " + p_class);
			}
		}

		return service;
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

		AbstractDXRAMComponent tmpComponent = m_components.get(p_class.getName());
		if (tmpComponent == null) {
			// check for any kind of instance of the specified class
			// we might have another interface/abstract class between the
			// class we request and an instance we could serve
			for (Entry<String, AbstractDXRAMComponent> entry : m_components.entrySet()) {
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

		if (component == null) {
			m_logger.warn(DXRAM_ENGINE_LOG_HEADER,
					"Getting component '" + p_class.getSimpleName() + "', not available.");
		}

		return component;
	}

	/**
	 * Get the configuration of the engine.
	 *
	 * @return Configuration or null if engine is not initialized.
	 */
	Configuration getConfiguration() {
		return m_configuration;
	}

	/**
	 * Get the settings instance of the engine.
	 *
	 * @return Settings instance or null if engine is not initialized.
	 */
	Settings getSettings() {
		return m_settings;
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
	 * Initialize the engine. Executes various bootstrapping tasks,
	 * initializes components and services.
	 * This will expect the most essential parameters to start to be provided
	 * via vm arguments (dxram.config, dxram.network.ip, dxram.network.port, dxram.role)
	 *
	 * @return True if successful, false otherwise.
	 */
	public boolean init() {
		return init(null, null, null, new String[0]);
	}

	/**
	 * Initialize the engine. Executes various bootstrapping tasks,
	 * initializes components and services.
	 *
	 * @param p_configurationFiles Absolute or relative path to one or multiple configuration files.
	 * @return True if successful, false otherwise.
	 */
	public boolean init(final String... p_configurationFiles) {
		return init(null, null, null, p_configurationFiles);
	}

	/**
	 * Initialize the engine. Executes various bootstrapping tasks,
	 * initializes components and services.
	 *
	 * @param p_overrideNetworkIP  Overriding the configuration file provided IP address (example: 127.0.0.1).
	 * @param p_overridePort       Overriding the configuration file provided port number (example: 22223).
	 * @param p_overrideRole       Overriding the configuration file provided role (example: Superpeer).
	 * @param p_configurationFiles Absolute or relative path to one or multiple configuration files.
	 * @return True if successful, false otherwise.
	 */
	public boolean init(final String p_overrideNetworkIP, final String p_overridePort,
			final NodeRole p_overrideRole, final String... p_configurationFiles) {
		assert !m_isInitilized;

		final List<AbstractDXRAMComponent> list;
		final Comparator<AbstractDXRAMComponent> comp;

		bootstrap(p_overrideNetworkIP, p_overridePort,
				p_overrideRole, p_configurationFiles);

		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Initializing engine...");

		setupJNI();

		m_logger.debug(DXRAM_ENGINE_LOG_HEADER, "Setting up components...");
		setupComponents(m_configuration);
		m_logger.debug(DXRAM_ENGINE_LOG_HEADER, "Setting up services...");
		setupServices(m_configuration);

		// sort list by initialization priority
		list = new ArrayList<AbstractDXRAMComponent>(m_components.values());
		comp = new Comparator<AbstractDXRAMComponent>() {
			@Override
			public int compare(final AbstractDXRAMComponent p_o1, final AbstractDXRAMComponent p_o2) {
				return (new Integer(p_o1.getPriorityInit())).compareTo(new Integer(p_o2.getPriorityInit()));
			}
		};
		Collections.sort(list, comp);

		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Initializing " + list.size() + " components...");
		for (AbstractDXRAMComponent component : list) {
			if (!component.init(this)) {
				m_logger.error(DXRAM_ENGINE_LOG_HEADER,
						"Initializing component '" + component.getComponentName() + "' failed, aborting init.");
				return false;
			}
		}
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Initializing components done.");

		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Starting " + m_services.size() + " services...");
		for (AbstractDXRAMService service : m_services.values()) {
			if (!service.start(this)) {
				m_logger.error(DXRAM_ENGINE_LOG_HEADER,
						"Starting service '" + service.getServiceName() + "' failed, aborting init.");
				return false;
			}
		}
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Starting services done.");

		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Initializing engine done.");
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

		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down engine...");

		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down " + m_services.size() + " services...");
		for (AbstractDXRAMService service : m_services.values()) {
			if (!service.shutdown()) {
				m_logger.error(DXRAM_ENGINE_LOG_HEADER,
						"Shutting down service '" + service.getServiceName() + "' failed.");
			}
		}
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down services done.");

		list = new ArrayList<AbstractDXRAMComponent>(m_components.values());

		comp = new Comparator<AbstractDXRAMComponent>() {
			@Override
			public int compare(final AbstractDXRAMComponent p_o1, final AbstractDXRAMComponent p_o2) {
				return (new Integer(p_o1.getPriorityShutdown())).compareTo(new Integer(p_o2.getPriorityShutdown()));
			}
		};

		Collections.sort(list, comp);

		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down " + list.size() + " components...");

		for (AbstractDXRAMComponent component : list) {
			component.shutdown();
		}

		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down components done.");

		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down engine done.");

		m_logger.close();

		m_isInitilized = false;

		return true;
	}

	/**
	 * Setup components from a configuration.
	 *
	 * @param p_configuration Configuration to get the components from.
	 */
	private void setupComponents(final Configuration p_configuration) {
		final Map<Integer, String> componentsClass =
				p_configuration.getValues("/DXRAMEngine/Components/Component/Class", String.class);
		final Map<Integer, Boolean> componentsEnabled =
				p_configuration.getValues("/DXRAMEngine/Components/Component/Enabled", Boolean.class);
		final Map<Integer, Integer> componentsPriorityInit =
				p_configuration.getValues("/DXRAMEngine/Components/Component/PriorityInit", Integer.class);
		final Map<Integer, Integer> componentsPriorityShutdown =
				p_configuration.getValues("/DXRAMEngine/Components/Component/PriorityShutdown", Integer.class);

		if (componentsClass != null) {
			for (Entry<Integer, String> component : componentsClass.entrySet()) {
				final Boolean enabled = componentsEnabled.get(component.getKey());
				if (enabled != null && enabled.booleanValue()) {
					final Integer priorityInit = componentsPriorityInit.get(component.getKey());
					if (priorityInit == null) {
						m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Cannot setup component " + component.getValue()
								+ " missing init priority, component ignored.");
						continue;
					}

					final Integer priorityShutdown = componentsPriorityShutdown.get(component.getKey());
					if (priorityShutdown == null) {
						m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Cannot setup component " + component.getValue()
								+ " missing uninit priority, component ignored.");
						continue;
					}

					Class<?> clazz = null;
					try {
						clazz = Class.forName(component.getValue());
					} catch (final ClassNotFoundException e) {
						m_logger.error(DXRAM_ENGINE_LOG_HEADER,
								"Could not find class " + component.getValue() + " in runtime, component ignored.");
						continue;
					}

					if (!clazz.getSuperclass().equals(AbstractDXRAMComponent.class)) {
						// check if there is an "interface"/abstract class between DXRAMComponent and the instance to
						// create
						if (!clazz.getSuperclass().getSuperclass().equals(AbstractDXRAMComponent.class)) {
							m_logger.error(DXRAM_ENGINE_LOG_HEADER, "DXRAMComponent is not a superclass of "
									+ component.getValue() + ", component ignored.");
							continue;
						}
					}

					Constructor<?> ctor = null;

					try {
						ctor = clazz.getConstructor(Integer.TYPE, Integer.TYPE);
					} catch (final NoSuchMethodException | SecurityException e1) {
						m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Could not get constructor of component "
								+ component.getValue() + " invalid constructor, component ignored.");
						continue;
					}

					try {
						m_components.put(clazz.getName(),
								(AbstractDXRAMComponent) ctor
										.newInstance(new Object[] {priorityInit, priorityShutdown}));
					} catch (final InstantiationException | IllegalAccessException | IllegalArgumentException
							| InvocationTargetException e) {
						if (Modifier.isAbstract(clazz.getModifiers())) {
							m_logger.error(DXRAM_ENGINE_LOG_HEADER,
									"Component '" + component.getValue() + "' is an abstract class.");
						}
						m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Could not create instance of component "
								+ component.getValue() + ", component ignored.");
						continue;
					}

					m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Component " + component.getValue() + " enabled.");
				} else {
					m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Component " + component.getValue() + " disabled.");
				}
			}
		}
	}

	/**
	 * Setup services from a configuration.
	 *
	 * @param p_configuration Configuration to get the services from.
	 */
	private void setupServices(final Configuration p_configuration) {
		final Map<Integer, String> servicesClass =
				p_configuration.getValues("/DXRAMEngine/Services/Service/Class", String.class);
		final Map<Integer, Boolean> servicesEnabled =
				p_configuration.getValues("/DXRAMEngine/Services/Service/Enabled", Boolean.class);

		if (servicesClass == null) {
			m_logger.error(DXRAM_ENGINE_LOG_HEADER,
					"Setting up services, service class list in configuration " + p_configuration.getName());
		}

		for (Entry<Integer, String> service : servicesClass.entrySet()) {
			final Boolean enabled = servicesEnabled.get(service.getKey());
			if (enabled != null && enabled.booleanValue()) {
				Class<?> clazz = null;
				try {
					clazz = Class.forName(service.getValue());
				} catch (final ClassNotFoundException e) {
					m_logger.error(DXRAM_ENGINE_LOG_HEADER,
							"Could not find class " + service.getValue() + " in runtime, service ignored.");
					continue;
				}

				if (!clazz.getSuperclass().equals(AbstractDXRAMService.class)) {
					// check if there is an "interface"/abstract class between DXRAMService and the instance to create
					if (!clazz.getSuperclass().getSuperclass().equals(AbstractDXRAMService.class)) {
						m_logger.error(DXRAM_ENGINE_LOG_HEADER,
								"DXRAMService is not a superclass of " + service.getValue() + ", service ignored.");
						continue;
					}
				}

				try {
					m_services.put(clazz.getName(), (AbstractDXRAMService) clazz.newInstance());
				} catch (final InstantiationException | IllegalAccessException e) {
					m_logger.error(DXRAM_ENGINE_LOG_HEADER,
							"Could not create instance of service " + service.getValue() + ", service ignored.");
					continue;
				}

				m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Service " + service.getValue() + " enabled.");
			} else {
				m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Service " + service.getValue() + " disabled.");
			}
		}
	}

	/**
	 * Execute bootstrapping tasks for the engine.
	 *
	 * @param p_overrideNetworkIP  Overriding the configuration file provided IP address (example: 127.0.0.1).
	 * @param p_overridePort       Overriding the configuration file provided port number (example: 22223).
	 * @param p_overrideRole       Overriding the configuration file provided role (example: Superpeer).
	 * @param p_configurationFiles Absolute or relative path to one or multiple configuration files.
	 */
	private void bootstrap(final String p_overrideNetworkIP, final String p_overridePort,
			final NodeRole p_overrideRole, final String... p_configurationFiles) {
		String[] configurationFiles = p_configurationFiles;
		final ArrayList<String> configurations = new ArrayList<String>();

		// check for configuration items dxram.config
		// or dxram.config.0, dxram.config.1 etc
		final String[] keyValue;
		keyValue = new String[2];

		keyValue[0] = "dxram.config";
		keyValue[1] = System.getProperty(keyValue[0]);
		if (keyValue[1] != null) {
			configurations.add(keyValue[1]);
		}

		// allow a max of 100 configs (should be plenty enough)
		for (int i = 0; i < 100; i++) {
			keyValue[0] = "dxram.config." + i;
			keyValue[1] = System.getProperty(keyValue[0]);
			// break if don't have a chain of configs
			if (keyValue[1] == null) {
				break;
			}

			configurations.add(keyValue[1]);
		}

		if (configurations.isEmpty() && configurationFiles.length == 0) {
			System.out.println("[WARN][DXRAMEngine] No dxram configuration specified via vm args.");
		}

		// override configuration files provided via api call with vm arguments
		// if any vm args exist
		if (!configurations.isEmpty()) {
			configurationFiles = configurations.toArray(new String[0]);
		}

		m_configuration = new Configuration("DXRAMEngine");

		// overriding order:
		// config, default values, class parameters, vm arguments
		for (String configFile : configurationFiles) {
			System.out.println("[INFO][DXRAMEngine] Loading configuration file " + configFile);
			final int configLoadSuccessful = loadConfiguration(configFile);
			if (configLoadSuccessful != 0) {
				System.out.println(
						"[ERR][DXRAMEngine] Loading from configuration file failed: could not find configuration file.");
			}
		}

		// default configuration values from engine (if values don't exist)
		m_settings = new Settings(m_configuration, m_logger);
		registerDefaultConfigurationValues();

		setupLogger();

		// parameters
		overrideConfigurationWithParameters(p_overrideNetworkIP, p_overridePort, p_overrideRole);

		// vm arguments
		overrideConfigurationWithVMArguments();

		m_logger.debug(DXRAM_ENGINE_LOG_HEADER, m_configuration.toString());

		// setup components and services
		setupComponents(m_configuration);
		setupServices(m_configuration);
	}

	/**
	 * Register default configuration values of the engine.
	 */
	private void registerDefaultConfigurationValues() {
		m_settings.setDefaultValue(DXRAMEngineConfigurationValues.IP);
		m_settings.setDefaultValue(DXRAMEngineConfigurationValues.PORT);
		m_settings.setDefaultValue(DXRAMEngineConfigurationValues.ROLE);
		m_settings.setDefaultValue(DXRAMEngineConfigurationValues.LOGGER_LEVEL);
		m_settings.setDefaultValue(DXRAMEngineConfigurationValues.LOGGER_FILE_LEVEL);
		m_settings.setDefaultValue(DXRAMEngineConfigurationValues.LOGGER_FILE_PATH);
		m_settings.setDefaultValue(DXRAMEngineConfigurationValues.LOGGER_FILE_BACKUPOLD);
		m_settings.setDefaultValue(DXRAMEngineConfigurationValues.LOGGER_CONSOLE_LEVEL);
		m_settings.setDefaultValue(DXRAMEngineConfigurationValues.PERFORMANCE_FLAG);
	}

	/**
	 * Load the configuration from a file.
	 *
	 * @param p_configurationFile Path to the configuration file.
	 * @return 0 if successful, -1 if loading from an existing file failed, 1 if configuration file
	 * does not exist and default file needs to be created/saved.
	 */
	private int loadConfiguration(final String p_configurationFile) {
		final ConfigurationXMLLoader loader = new ConfigurationXMLLoaderFile(p_configurationFile);
		final ConfigurationParser parser = new ConfigurationXMLParser(loader);

		try {
			parser.readConfiguration(m_configuration);
		} catch (final ConfigurationException e) {
			// check if file exists -> save default config later
			if (new File(p_configurationFile).exists()) {
				return 1;
			} else {
				return -1;
			}
		}

		return 0;
	}

	/**
	 * Save the configuration to a file.
	 *
	 * @param p_configurationFolder File to save the configuration to.
	 * @return True if saving was successful, false otherwise.
	 */
	@SuppressWarnings("unused")
	private boolean saveConfiguration(final String p_configurationFolder) {
		final ConfigurationXMLLoader loader = new ConfigurationXMLLoaderFile(p_configurationFolder);
		final ConfigurationParser parser = new ConfigurationXMLParser(loader);

		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Saving configuration: " + loader);
		try {
			parser.writeConfiguration(m_configuration);
		} catch (final ConfigurationException e) {
			m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Writing configuration file '" + loader + "' failed.", e);
			return false;
		}

		return true;
	}

	/**
	 * Override a few configuration values with the provided paramters.
	 *
	 * @param p_networkIP Network IP of the instance.
	 * @param p_port      Port number of the instance.
	 * @param p_role      Role of the instance.
	 */
	private void overrideConfigurationWithParameters(final String p_networkIP,
			final String p_port, final NodeRole p_role) {
		if (p_networkIP != null) {
			m_settings.overrideValue(DXRAMEngineConfigurationValues.IP, p_networkIP);
		}
		if (p_port != null) {
			m_settings.overrideValue(DXRAMEngineConfigurationValues.PORT, Integer.parseInt(p_port));
		}
		if (p_role != null) {
			m_settings.overrideValue(DXRAMEngineConfigurationValues.ROLE, p_role.toString());
		}
	}

	/**
	 * Override a few configuration values with parameters provided by the VM arguments.
	 */
	private void overrideConfigurationWithVMArguments() {
		final String[] keyValue;

		keyValue = new String[2];
		keyValue[0] = "dxram.network.ip";
		keyValue[1] = System.getProperty(keyValue[0]);
		if (keyValue[1] != null) {
			m_logger.debug(DXRAM_ENGINE_LOG_HEADER,
					"Overriding '" + keyValue[0] + "' with vm argument '" + keyValue[1] + "'.");
			m_settings.overrideValue(DXRAMEngineConfigurationValues.IP, keyValue[1]);
		}

		keyValue[0] = "dxram.network.port";
		keyValue[1] = System.getProperty(keyValue[0]);
		if (keyValue[1] != null) {
			m_logger.debug(DXRAM_ENGINE_LOG_HEADER,
					"Overriding '" + keyValue[0] + "' with vm argument '" + keyValue[1] + "'.");
			m_settings.overrideValue(DXRAMEngineConfigurationValues.PORT, Integer.parseInt(keyValue[1]));
		}

		keyValue[0] = "dxram.role";
		keyValue[1] = System.getProperty(keyValue[0]);
		if (keyValue[1] != null) {
			m_logger.debug(DXRAM_ENGINE_LOG_HEADER,
					"Overriding '" + keyValue[0] + "' with vm argument '" + keyValue[1] + "'.");
			m_settings.overrideValue(DXRAMEngineConfigurationValues.ROLE, keyValue[1]);
		}
	}

	/**
	 * Setup the logger.
	 */
	private void setupLogger() {
		final String loggerLevel = m_settings.getValue(DXRAMEngineConfigurationValues.LOGGER_LEVEL);
		final String loggerFileLevel = m_settings.getValue(DXRAMEngineConfigurationValues.LOGGER_FILE_LEVEL);
		final String loggerFilePath = m_settings.getValue(DXRAMEngineConfigurationValues.LOGGER_FILE_PATH);
		final String loggerConsoleLevel = m_settings.getValue(DXRAMEngineConfigurationValues.LOGGER_CONSOLE_LEVEL);
		final Boolean loggerFileBackupOld = m_settings.getValue(DXRAMEngineConfigurationValues.LOGGER_FILE_BACKUPOLD);

		m_logger = new Logger();
		m_logger.setLogLevel(LogLevel.toLogLevel(loggerLevel));
		{
			final LogDestination logDest = new LogDestinationConsole();
			m_logger.addLogDestination(logDest, LogLevel.toLogLevel(loggerConsoleLevel));
		}
		{
			final LogDestination logDest = new LogDestinationFile(loggerFilePath, loggerFileBackupOld);
			m_logger.addLogDestination(logDest, LogLevel.toLogLevel(loggerFileLevel));
		}
	}

	/**
	 * Setup JNI related stuff.
	 */
	private void setupJNI() {
		m_jniManager = new DXRAMJNIManager(m_logger);
		m_jniManager.setup(m_settings);
	}

	/**
	 * Settings subclass to provide settings for the engine. Wraps a configuration.
	 *
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
	 */
	public static class Settings {
		private static final String DXRAM_ENGINE_LOG_HEADER = DXRAMEngine.class.getSimpleName();

		private Configuration m_configuration;
		private Logger m_logger;
		private String m_basePath = new String();

		/**
		 * Constructor
		 *
		 * @param p_configuration Configuration to wrap, which contains engine settings.
		 * @param p_logger        Logger to use for logging messages.
		 */
		Settings(final Configuration p_configuration, final Logger p_logger) {
			m_configuration = p_configuration;
			m_logger = p_logger;
			m_basePath = "/DXRAMEngine/Settings/";
		}

		/**
		 * Override existing configuration values.
		 *
		 * @param <T>             Type of the value.
		 * @param p_default       Pair of key of the value to override and default value for the key.
		 * @param p_overrideValue True to override if the value exists, false to not override if exists.
		 */
		public <T> void overrideValue(final Pair<String, T> p_default, final T p_overrideValue) {
			m_configuration.addValue(m_basePath + p_default.first(), p_overrideValue, true);
		}

		/**
		 * Set a default value for a specific configuration key.
		 *
		 * @param <T>       Type of the value.
		 * @param p_default Pair of configuration key and default value to set for the specified key.
		 */
		public <T> void setDefaultValue(final Pair<String, T> p_default) {
			setDefaultValue(p_default.first(), p_default.second());
		}

		/**
		 * Set a default value for a specific configuration key.
		 *
		 * @param <T>     Type of the value.
		 * @param p_key   Key for the value.
		 * @param p_value Value to set.
		 */
		public <T> void setDefaultValue(final String p_key, final T p_value) {
			if (m_configuration.addValue(m_basePath + p_key, p_value, false)) {
				// we added a default value => value was missing from configuration
				if (m_logger != null) {
					m_logger.warn(DXRAM_ENGINE_LOG_HEADER,
							"Settings value for '" + p_key + "' is missing in " + m_basePath + ", using default value "
									+ p_value);
				}
			}
		}

		/**
		 * Get a value from the configuration for the component.
		 *
		 * @param <T>       Type of the value.
		 * @param p_default Pair of key and default value to get value for.
		 * @return Value associated with the provided key.
		 */
		@SuppressWarnings("unchecked")
		public <T> T getValue(final Pair<String, T> p_default) {
			return (T) getValue(p_default.first(), p_default.second().getClass());
		}

		/**
		 * Get a value from the configuration for the component.
		 *
		 * @param <T>    Type of the value.
		 * @param p_key  Key to get the value for.
		 * @param p_type Type of the value to get.
		 * @return Value assicated with the provided key.
		 */
		public <T> T getValue(final String p_key, final Class<T> p_type) {
			return m_configuration.getValue(m_basePath + p_key, p_type);
		}
	}
}
