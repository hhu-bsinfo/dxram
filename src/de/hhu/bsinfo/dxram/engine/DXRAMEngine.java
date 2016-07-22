
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
import de.hhu.bsinfo.utils.reflect.dt.*;

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

			// #if LOGGER >= WARN
			if (service == null) {
				m_logger.warn(DXRAM_ENGINE_LOG_HEADER, "Service not available " + p_class);
			}
			// #endif /* LOGGER >= WARN */
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

		// #if LOGGER >= WARN
		if (component == null) {
			m_logger.warn(DXRAM_ENGINE_LOG_HEADER,
					"Getting component '" + p_class.getSimpleName() + "', not available.");
		}
		// #endif /* LOGGER >= WARN */

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

		// #if LOGGER >= INFO
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Initializing engine...");
		// #endif /* LOGGER >= INFO */

		setupJNI();

		// #if LOGGER >= DEBUG
		m_logger.debug(DXRAM_ENGINE_LOG_HEADER, "Setting up components...");
		setupComponents(m_configuration);
		m_logger.debug(DXRAM_ENGINE_LOG_HEADER, "Setting up services...");
		setupServices(m_configuration);
		// #endif /* LOGGER >= DEBUG */

		// sort list by initialization priority
		list = new ArrayList<AbstractDXRAMComponent>(m_components.values());
		comp = new Comparator<AbstractDXRAMComponent>() {
			@Override
			public int compare(final AbstractDXRAMComponent p_o1, final AbstractDXRAMComponent p_o2) {
				return (new Integer(p_o1.getPriorityInit())).compareTo(new Integer(p_o2.getPriorityInit()));
			}
		};
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
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Starting " + m_services.size() + " services...");
		// #endif /* LOGGER >= INFO */

		for (AbstractDXRAMService service : m_services.values()) {
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
		//
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down " + m_services.size() + " services...");
		// #endif /* LOGGER >= INFO */

		for (AbstractDXRAMService service : m_services.values()) {
			// #if LOGGER >= ERROR
			if (!service.shutdown()) {
				m_logger.error(DXRAM_ENGINE_LOG_HEADER,
						"Shutting down service '" + service.getServiceName() + "' failed.");
			}
			// #endif /* LOGGER >= ERROR */
		}
		// #if LOGGER >= INFO
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down services done.");
		// #endif /* LOGGER >= INFO */

		list = new ArrayList<AbstractDXRAMComponent>(m_components.values());

		comp = new Comparator<AbstractDXRAMComponent>() {
			@Override
			public int compare(final AbstractDXRAMComponent p_o1, final AbstractDXRAMComponent p_o2) {
				return (new Integer(p_o1.getPriorityShutdown())).compareTo(new Integer(p_o2.getPriorityShutdown()));
			}
		};

		Collections.sort(list, comp);

		// #if LOGGER >= INFO
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down " + list.size() + " components...");
		// #endif /* LOGGER >= INFO */

		for (AbstractDXRAMComponent component : list) {
			component.shutdown();
		}

		// #if LOGGER >= INFO
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down components done.");
		//
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down engine done.");
		// #endif /* LOGGER >= INFO */

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
						// #if LOGGER >= ERROR
						m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Cannot setup component " + component.getValue()
								+ " missing init priority, component ignored.");
						// #endif /* LOGGER >= ERROR */
						continue;
					}

					final Integer priorityShutdown = componentsPriorityShutdown.get(component.getKey());
					if (priorityShutdown == null) {
						// #if LOGGER >= ERROR
						m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Cannot setup component " + component.getValue()
								+ " missing uninit priority, component ignored.");
						// #endif /* LOGGER >= ERROR */
						continue;
					}

					Class<?> clazz = null;
					try {
						clazz = Class.forName(component.getValue());
					} catch (final ClassNotFoundException e) {
						// #if LOGGER >= ERROR
						m_logger.error(DXRAM_ENGINE_LOG_HEADER,
								"Could not find class " + component.getValue() + " in runtime, component ignored.");
						// #endif /* LOGGER >= ERROR */
						continue;
					}

					if (!clazz.getSuperclass().equals(AbstractDXRAMComponent.class)) {
						// check if there is an "interface"/abstract class between DXRAMComponent and the instance to
						// create
						if (!clazz.getSuperclass().getSuperclass().equals(AbstractDXRAMComponent.class)) {
							// #if LOGGER >= ERROR
							m_logger.error(DXRAM_ENGINE_LOG_HEADER, "DXRAMComponent is not a superclass of "
									+ component.getValue() + ", component ignored.");
							// #endif /* LOGGER >= ERROR */
							continue;
						}
					}

					Constructor<?> ctor = null;

					try {
						ctor = clazz.getConstructor(Integer.TYPE, Integer.TYPE);
					} catch (final NoSuchMethodException | SecurityException e1) {
						// #if LOGGER >= ERROR
						m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Could not get constructor of component "
								+ component.getValue() + " invalid constructor, component ignored.");
						// #endif /* LOGGER >= ERROR */
						continue;
					}

					try {
						m_components.put(clazz.getName(),
								(AbstractDXRAMComponent) ctor
										.newInstance(new Object[] {priorityInit, priorityShutdown}));
					} catch (final InstantiationException | IllegalAccessException | IllegalArgumentException
							| InvocationTargetException e) {
						// #if LOGGER >= ERROR
						if (Modifier.isAbstract(clazz.getModifiers())) {
							m_logger.error(DXRAM_ENGINE_LOG_HEADER,
									"Component '" + component.getValue() + "' is an abstract class.");
						}
						m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Could not create instance of component "
								+ component.getValue() + ", component ignored.");
						// #endif /* LOGGER >= ERROR */
						continue;
					}

					// #if LOGGER >= INFO
					m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Component " + component.getValue() + " enabled.");
					// #endif /* LOGGER >= INFO */
				} else {
					// #if LOGGER >= INFO
					m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Component " + component.getValue() + " disabled.");
					// #endif /* LOGGER >= INFO */
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
					// #if LOGGER >= ERROR
					m_logger.error(DXRAM_ENGINE_LOG_HEADER,
							"Could not find class " + service.getValue() + " in runtime, service ignored.");
					// #endif /* LOGGER >= ERROR */
					continue;
				}

				if (!clazz.getSuperclass().equals(AbstractDXRAMService.class)) {
					// check if there is an "interface"/abstract class between DXRAMService and the instance to create
					if (!clazz.getSuperclass().getSuperclass().equals(AbstractDXRAMService.class)) {
						// #if LOGGER >= ERROR
						m_logger.error(DXRAM_ENGINE_LOG_HEADER,
								"DXRAMService is not a superclass of " + service.getValue() + ", service ignored.");
						// #endif /* LOGGER >= ERROR */
						continue;
					}
				}

				try {
					m_services.put(clazz.getName(), (AbstractDXRAMService) clazz.newInstance());
				} catch (final InstantiationException | IllegalAccessException e) {
					// #if LOGGER >= ERROR
					m_logger.error(DXRAM_ENGINE_LOG_HEADER,
							"Could not create instance of service " + service.getValue() + ", service ignored.");
					// #endif /* LOGGER >= ERROR */
					continue;
				}

				// #if LOGGER >= INFO
				m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Service " + service.getValue() + " enabled.");
				// #endif /* LOGGER >= INFO */
			} else {
				// #if LOGGER >= INFO
				m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Service " + service.getValue() + " disabled.");
				// #endif /* LOGGER >= INFO */
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
		final String[] keyValue;
		keyValue = new String[2];

		keyValue[0] = "dxram.config";
		keyValue[1] = System.getProperty(keyValue[0]);
		if(keyValue[1] != null) {
			configurations.add(keyValue[1]);
		} else {
			System.out.println("[DXRAMEngine] No VM argument -Ddxram.config=... was entered! Please fix it and restart the application!");
			System.exit(1);
		}

		// allow a max of 100 configs (should be plenty enough)
		for(int i=0; i < 100; i++) {
			keyValue[0] = "dxram.config." + i;
			keyValue[1] = System.getProperty(keyValue[0]);
			// break if don't have a chain of configs
			if(keyValue[1] == null) {
				break;
			}

			configurations.add(keyValue[1]);
		}

		if(configurations.isEmpty() && configurationFiles.length == 0) {
			System.out.println("[WARN][DXRAMEngine] No dxram configuration specified via vm args.");
		}

		// override configuration files provided via api call with vm arguments
		// if any vm args exist
		if(!configurations.isEmpty()) {
			configurationFiles = configurations.toArray(new String[0]);
		}

		m_configuration = new Configuration("DXRAMEngine");

		boolean fileReadSuccessful = true;

		// load config files
		for (String configFile : configurationFiles) {
			System.out.println("[DXRAMEngine] Loading configuration file " + configFile);
			int configLoadSuccessful = loadConfiguration(configFile);
			if (configLoadSuccessful != 0) {
				System.out.println("[DXRAMEngine] Loading from configuration file failed: create default config.");
				fileReadSuccessful = false;
				break;

			}
		}


		m_settings = new Settings(m_configuration, m_logger);

		// default configuration values from engine (if values don't exist)
		if(!fileReadSuccessful) {
			initDefaultComponentsAndServices();

		}

		// vm arguments
		overrideConfigurationWithVMArguments();

		setupLogger();

		// #if LOGGER >= DEBUG
		// // m_logger.debug(DXRAM_ENGINE_LOG_HEADER, m_configuration.toString());
		// #endif /* LOGGER >= DEBUG */

		// setup components and services
		setupComponents(m_configuration);
		setupServices(m_configuration);

		// default configuration values from engine (if values don't exist)
		registerDefaultConfigurationValues();

		preInitComponents();
		preInitServices();

        // load remaining configs dxram.config.0, ...
		if(!fileReadSuccessful) {
			saveConfiguration(System.getProperty("dxram.config"));
			for (int i=1; i< configurationFiles.length; i++) { // 1 because vm argument was entered wrong
				System.out.println("[DXRAMEngine] Loading configuration file " + configurationFiles[i]);
				final int configLoadSuccessful = loadConfiguration(configurationFiles[i]);
				if (configLoadSuccessful != 0) {
					System.out.println("[DXRAMEngine] Loading from configuration file failed: could not find configuration file.");
				}
			}
		}

	}

    /**
     * Initialize components
     */
	private void preInitComponents() {
		for(Entry<String, AbstractDXRAMComponent> comp : m_components.entrySet()) {
			AbstractDXRAMComponent component = comp.getValue();
			component.preInit(this);
		}
	}

    /**
     * Initialize services.
     */
	private void preInitServices() {
		for(Entry<String, AbstractDXRAMService> serv : m_services.entrySet()) {
			AbstractDXRAMService service = serv.getValue();
			service.preInit(this);
		}
	}

	/**
	 *
	 */
	private void initDefaultComponentsAndServices() {
		// hard-coded with default values
		//components
		m_configuration.addValue("/DXRAMEngine/Components/Component/Class", 0, "de.hhu.bsinfo.dxram.engine.NullComponent");
		m_configuration.addValue("/DXRAMEngine/Components/Component/Class", 1, "de.hhu.bsinfo.dxram.logger.LoggerComponent");
		m_configuration.addValue("/DXRAMEngine/Components/Component/Class", 2, "de.hhu.bsinfo.dxram.term.TerminalComponent");
		m_configuration.addValue("/DXRAMEngine/Components/Component/Class", 3, "de.hhu.bsinfo.dxram.event.EventComponent");
		m_configuration.addValue("/DXRAMEngine/Components/Component/Class", 4, "de.hhu.bsinfo.dxram.stats.StatisticsComponent");
		m_configuration.addValue("/DXRAMEngine/Components/Component/Class", 5, "de.hhu.bsinfo.dxram.boot.ZookeeperBootComponent");
		m_configuration.addValue("/DXRAMEngine/Components/Component/Class", 6, "de.hhu.bsinfo.dxram.net.NetworkComponent");
		m_configuration.addValue("/DXRAMEngine/Components/Component/Class", 7, "de.hhu.bsinfo.dxram.lookup.LookupComponent");
		m_configuration.addValue("/DXRAMEngine/Components/Component/Class", 8, "de.hhu.bsinfo.dxram.mem.MemoryManagerComponent");
		m_configuration.addValue("/DXRAMEngine/Components/Component/Class", 9, "de.hhu.bsinfo.dxram.lock.PeerLockComponent");
		m_configuration.addValue("/DXRAMEngine/Components/Component/Class",10, "de.hhu.bsinfo.dxram.log.LogComponent");
		m_configuration.addValue("/DXRAMEngine/Components/Component/Class",11, "de.hhu.bsinfo.dxram.backup.BackupComponent");
		m_configuration.addValue("/DXRAMEngine/Components/Component/Class",12, "de.hhu.bsinfo.dxram.chunk.ChunkComponent");
		m_configuration.addValue("/DXRAMEngine/Components/Component/Class",13, "de.hhu.bsinfo.dxram.nameservice.NameserviceComponent");
		//enabled and priority flags flags for components
		for(int i=0; i<=13; i++) {
			m_configuration.addValue("/DXRAMEngine/Components/Component/Enabled", i, Boolean.TRUE);
			m_configuration.addValue("/DXRAMEngine/Components/Component/PriorityInit", i, i);
		}
		m_configuration.addValue("/DXRAMEngine/Components/Component/PriorityShutdown", 0, 999);
		m_configuration.addValue("/DXRAMEngine/Components/Component/PriorityShutdown", 1, 998);
		m_configuration.addValue("/DXRAMEngine/Components/Component/PriorityShutdown", 2, 997);
		m_configuration.addValue("/DXRAMEngine/Components/Component/PriorityShutdown", 3, 996);
		m_configuration.addValue("/DXRAMEngine/Components/Component/PriorityShutdown", 4, 995);
		m_configuration.addValue("/DXRAMEngine/Components/Component/PriorityShutdown", 5, 986);
		m_configuration.addValue("/DXRAMEngine/Components/Component/PriorityShutdown", 6, 994);
		m_configuration.addValue("/DXRAMEngine/Components/Component/PriorityShutdown", 7, 993);
		m_configuration.addValue("/DXRAMEngine/Components/Component/PriorityShutdown", 8, 992);
		m_configuration.addValue("/DXRAMEngine/Components/Component/PriorityShutdown", 9, 991);
		m_configuration.addValue("/DXRAMEngine/Components/Component/PriorityShutdown",10, 990);
		m_configuration.addValue("/DXRAMEngine/Components/Component/PriorityShutdown",11, 989);
		m_configuration.addValue("/DXRAMEngine/Components/Component/PriorityShutdown",12, 988);
		m_configuration.addValue("/DXRAMEngine/Components/Component/PriorityShutdown",13, 987);


		//services
		m_configuration.addValue("/DXRAMEngine/Services/Service/Class", 0, "de.hhu.bsinfo.dxram.engine.NullService");
		m_configuration.addValue("/DXRAMEngine/Services/Service/Class", 1, "de.hhu.bsinfo.dxram.chunk.ChunkService");
		m_configuration.addValue("/DXRAMEngine/Services/Service/Class", 2, "de.hhu.bsinfo.dxram.nameservice.NameserviceService");
		m_configuration.addValue("/DXRAMEngine/Services/Service/Class", 3, "de.hhu.bsinfo.dxram.lock.PeerLockService");
		m_configuration.addValue("/DXRAMEngine/Services/Service/Class", 4, "de.hhu.bsinfo.dxram.logger.LoggerService");
		m_configuration.addValue("/DXRAMEngine/Services/Service/Class", 5, "de.hhu.bsinfo.dxram.term.TerminalService");
		m_configuration.addValue("/DXRAMEngine/Services/Service/Class", 6, "de.hhu.bsinfo.dxram.boot.BootService");
		m_configuration.addValue("/DXRAMEngine/Services/Service/Class", 7, "de.hhu.bsinfo.dxram.net.NetworkService");
		m_configuration.addValue("/DXRAMEngine/Services/Service/Class", 8, "de.hhu.bsinfo.dxram.stats.StatisticsService");
		m_configuration.addValue("/DXRAMEngine/Services/Service/Class", 9, "de.hhu.bsinfo.dxram.chunk.AsyncChunkService");
		m_configuration.addValue("/DXRAMEngine/Services/Service/Class",10, "de.hhu.bsinfo.dxram.migration.MigrationService");
		m_configuration.addValue("/DXRAMEngine/Services/Service/Class",11, "de.hhu.bsinfo.dxram.log.LogService");
		m_configuration.addValue("/DXRAMEngine/Services/Service/Class",12, "de.hhu.bsinfo.dxram.sync.SynchronizationService");
		//enabled flags for servies
		for(int i=0; i<=12; i++) {
			m_configuration.addValue("/DXRAMEngine/Services/Service/Enabled", i, Boolean.TRUE);
		}

		// default configutation values from engine (if values don't exist)
		registerDefaultConfigurationValues();

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
	private boolean saveConfiguration(final String p_configurationFolder) {
		final ConfigurationXMLLoader loader = new ConfigurationXMLLoaderFile(p_configurationFolder);
		final ConfigurationParser parser = new ConfigurationXMLParser(loader);

		// #if LOGGER >= INFO
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Saving configuration: " + loader);
		// #endif /* LOGGER >= INFO */
		try {
			parser.writeConfiguration(m_configuration);
		} catch (final ConfigurationException e) {
			// #if LOGGER >= ERROR
			m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Writing configuration file '" + loader + "' failed.", e);
			// #endif /* LOGGER >= ERROR */
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

        Map<String, DataTypeParser> m_dataTypeParsers = new HashMap<String, DataTypeParser>();
        // add default type parsers
        addDataTypeParser(m_dataTypeParsers, new DataTypeParserString());
        addDataTypeParser(m_dataTypeParsers, new DataTypeParserByte());
        addDataTypeParser(m_dataTypeParsers, new DataTypeParserShort());
        addDataTypeParser(m_dataTypeParsers, new DataTypeParserInt());
        addDataTypeParser(m_dataTypeParsers, new DataTypeParserLong());
        addDataTypeParser(m_dataTypeParsers, new DataTypeParserFloat());
        addDataTypeParser(m_dataTypeParsers, new DataTypeParserDouble());
        addDataTypeParser(m_dataTypeParsers, new DataTypeParserBool());
        addDataTypeParser(m_dataTypeParsers, new DataTypeParserBoolean());

        for(int i=0; i<100; i++) {
            keyValue[0] = "dxram.confVal." + i;
            keyValue[1] = System.getProperty(keyValue[0]);
            if (keyValue[1] == null) {
                break;
            }else {
                String[] items = keyValue[1].split("#");
                if(items.length == 3) { // name#type#val
                    DataTypeParser parser = m_dataTypeParsers.get(items[1]);
                    if (parser != null) {
                        Object value = parser.parse(items[2]);

                        // add the value and do replace existing values
                        m_configuration.addValue(items[0], 0, value, true);
                    }
                    // no parser to support, ignore

                    // #if LOGGER >= DEBUG
                    // m_logger.debug(DXRAM_ENGINE_LOG_HEADER,
                    //		"Overriding '" + items[0] + "' with vm argument '" + items[2] + "'.");
                    // #endif /* LOGGER >= DEBUG */
                } else if(items.length == 4) { // name#id#type#val
                    int id = Integer.parseInt(items[1]);

                    DataTypeParser parser = m_dataTypeParsers.get(items[2]);
                    if (parser != null) {
                        Object value = parser.parse(items[3]);

                        // add the value and do replace existing values
                        m_configuration.addValue(items[0], 0, value, true);
                    }
                    // #if LOGGER >= DEBUG
                    // m_logger.debug(DXRAM_ENGINE_LOG_HEADER,
                    //		"Overriding '" + items[0] + "' with vm argument '" + items[2] + "'.");
                    // #endif /* LOGGER >= DEBUG */
                } else {
                    System.out.println("[DXRAMEngine] VM Argument error: invalid format");

                }
            }

        }
	}

    private boolean addDataTypeParser(Map<String, DataTypeParser> m_dataTypeParsers, DataTypeParser p_parser) {
        return m_dataTypeParsers.put(p_parser.getTypeIdentifer(), p_parser) == null;
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
				// #if LOGGER >= WARN
				if (m_logger != null) {
					m_logger.warn(DXRAM_ENGINE_LOG_HEADER,
							"Settings value for '" + p_key + "' is missing in " + m_basePath + ", using default value "
									+ p_value);
				}
				// #endif /* LOGGER >= WARN */
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
