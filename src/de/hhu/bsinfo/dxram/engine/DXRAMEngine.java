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

import de.hhu.bsinfo.dxram.util.logger.LogLevel;
import de.hhu.bsinfo.dxram.util.logger.Logger;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.conf.Configuration;
import de.hhu.bsinfo.utils.conf.ConfigurationException;
import de.hhu.bsinfo.utils.conf.ConfigurationParser;
import de.hhu.bsinfo.utils.conf.ConfigurationXMLLoader;
import de.hhu.bsinfo.utils.conf.ConfigurationXMLLoaderFile;
import de.hhu.bsinfo.utils.conf.ConfigurationXMLParser;
import de.hhu.bsinfo.utils.locks.JNILock;

public class DXRAMEngine 
{
	public static class Settings
	{
		private final String DXRAM_ENGINE_LOG_HEADER = this.getClass().getSimpleName();
		
		private Configuration m_configuration = null;
		private Logger m_logger = null;
		private String m_basePath = new String();
		
		Settings(final Configuration p_configuration, final Logger p_logger)
		{
			m_configuration = p_configuration;
			m_logger = p_logger;
			m_basePath = "/DXRAMEngine/Settings/";
		}
		
		public <T> void overrideValue(final Pair<String, T> p_default, final T p_overrideValue)
		{
			m_configuration.addValue(m_basePath + p_default.first(), p_overrideValue, true);
		}
		
		public <T> void setDefaultValue(final Pair<String, T> p_default)
		{
			setDefaultValue(p_default.first(), p_default.second());
		}
		
		public <T> void setDefaultValue(final String p_key, final T p_value)
		{
			if (m_configuration.addValue(m_basePath + p_key, p_value, false))
			{
				// we added a default value => value was missing from configuration
				if (m_logger != null)
					m_logger.warn(DXRAM_ENGINE_LOG_HEADER, "Settings value for '" + p_key + "' was missing, using default value " + p_value);
			}
		}
		
		@SuppressWarnings("unchecked")
		public <T> T getValue(final Pair<String, T> p_default)
		{
			return (T) getValue(p_default.first(), p_default.second().getClass());
		}
		
		public <T> T getValue(final String p_key, final Class<T> p_type)
		{
			return m_configuration.getValue(m_basePath + p_key, p_type);
		}
	}
	
	private final String DXRAM_ENGINE_LOG_HEADER = this.getClass().getSimpleName();
	
	private boolean m_isInitilized = false;
	
	private Configuration m_configuration = null;
	private Settings m_settings = null;
	private Logger m_logger = null;
	
	private HashMap<String, DXRAMComponent> m_components = new HashMap<String, DXRAMComponent>();
	private HashMap<String, DXRAMService> m_services = new HashMap<String, DXRAMService>();
	
	public DXRAMEngine()
	{		

	}
	
	public <T extends DXRAMService> T getService(final Class<T> p_class)
	{
		T service = null;
		
		if (m_isInitilized) {
			DXRAMService tmpService = m_services.get(p_class.getName());
			if (tmpService != null && p_class.isInstance(tmpService)) {
				service = p_class.cast(tmpService);
			}
			
			if (service == null) {
				m_logger.warn(DXRAM_ENGINE_LOG_HEADER, "Service not available " + p_class);
			}
		}
		
		return service;
	}
	
	<T extends DXRAMComponent> T getComponent(final Class<T> p_class)
	{
		T component = null;
	
		DXRAMComponent tmpComponent = m_components.get(p_class.getName());
		if (tmpComponent == null)
		{
			// check for any kind of instance of the specified class
			// we might have another interface/abstract class between the 
			// class we request and an instance we could serve
			for (Entry<String, DXRAMComponent> entry : m_components.entrySet())
			{
				tmpComponent = entry.getValue();
				if (p_class.isInstance(tmpComponent))
					break;
				
				tmpComponent = null;
			}
		}
		
		if (tmpComponent != null && p_class.isInstance(tmpComponent)) {
			component = p_class.cast(tmpComponent);
		}
		
		if (component == null)
			m_logger.warn(DXRAM_ENGINE_LOG_HEADER, "Getting component '" + p_class.getSimpleName() + "', not available.");
		
		return component;
	}
	
	Configuration getConfiguration()
	{
		return m_configuration;
	}
	
	Settings getSettings()
	{
		return m_settings;
	}
	
	Logger getLogger()
	{
		return m_logger;
	}
	
	public boolean init(final String p_configurationFile) {
		return init(p_configurationFile, null, null, null);
	}
	
	public boolean init(final String p_configurationFile, final String p_overrideNetworkIP, 
			final String p_overridePort, final String p_overrideRole)
	{
		assert !m_isInitilized;
		
		List<DXRAMComponent> list;
		Comparator<DXRAMComponent> comp;
		
		bootstrap(p_configurationFile, p_overrideNetworkIP, 
				p_overridePort, p_overrideRole);
		
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Initializing engine...");
		
		setupJNI();
		
		m_logger.debug(DXRAM_ENGINE_LOG_HEADER, "Setting up components...");
		setupComponents(m_configuration);
		m_logger.debug(DXRAM_ENGINE_LOG_HEADER, "Setting up services...");
		setupServices(m_configuration);
		
		// sort list by initialization priority
        list = new ArrayList<DXRAMComponent>(m_components.values());
        comp = new Comparator<DXRAMComponent>() {
            @Override
            public int compare(DXRAMComponent o1, DXRAMComponent o2) {
                return (new Integer(o1.getPriorityInit())).compareTo(new Integer(o2.getPriorityInit()));
            }
        };
        Collections.sort(list, comp);
        
        m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Initializing " + list.size() + " components...");
        for (DXRAMComponent component : list) {
            if (component.init(this) == false) {
            	m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Initializing component '" + component.getComponentName() + "' failed, aborting init.");
                return false;
            }
        }
        m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Initializing components done.");
        
        m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Starting " + m_services.size() + " services...");
        for (DXRAMService service : m_services.values()) {
            if (service.start(this) == false) {
            	m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Starting service '" + service.getServiceName() + "' failed, aborting init.");
                return false;
            }
        }
        m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Starting services done.");

        m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Initializing engine done.");
        m_isInitilized = true;
		
		return true;
	}
	
	public boolean shutdown()
	{
		assert m_isInitilized;

		List<DXRAMComponent> list;
		Comparator<DXRAMComponent> comp;
		
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down engine...");

		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down " + m_services.size() + " services...");
        for (DXRAMService service : m_services.values()) {
            if (service.shutdown() == false) {
            	m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Shutting down service '" + service.getServiceName() + "' failed.");
            }
        }
        m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down services done.");
		
		list = new ArrayList<DXRAMComponent>(m_components.values());

	    comp = new Comparator<DXRAMComponent>() {
            @Override
            public int compare(DXRAMComponent o1, DXRAMComponent o2) {
                return (new Integer(o1.getPriorityShutdown())).compareTo(new Integer(o2.getPriorityShutdown()));
            }
        };

        Collections.sort(list, comp);
        
        m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down " + list.size() + " components...");

        for (DXRAMComponent component : list) {
        	component.shutdown();
        }
        
        m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down components done.");

        m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Shutting down engine done.");

        m_isInitilized = false;

		return true;
	}
	
	private void setupComponents(final Configuration p_configuration)
	{
		Map<Integer, String> componentsClass = p_configuration.getValues("/DXRAMEngine/Components/Component/Class", String.class);
		Map<Integer, Boolean> componentsEnabled = p_configuration.getValues("/DXRAMEngine/Components/Component/Enabled", Boolean.class);
		Map<Integer, Integer> componentsPriorityInit = p_configuration.getValues("/DXRAMEngine/Components/Component/PriorityInit", Integer.class);
		Map<Integer, Integer> componentsPriorityShutdown = p_configuration.getValues("/DXRAMEngine/Components/Component/PriorityShutdown", Integer.class);
		
		if (componentsClass != null)
		{
			for (Entry<Integer, String> component : componentsClass.entrySet())
			{
				Boolean enabled = componentsEnabled.get(component.getKey());
				if (enabled != null && enabled.booleanValue())
				{
					Integer priorityInit = componentsPriorityInit.get(component.getKey());
					if (priorityInit == null)
					{
						m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Cannot setup component " + component.getValue() + " missing init priority, component ignored.");
						continue;
					}
					
					Integer priorityShutdown = componentsPriorityShutdown.get(component.getKey());
					if (priorityShutdown == null)
					{
						m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Cannot setup component " + component.getValue() + " missing uninit priority, component ignored.");
						continue;
					}
					
					Class<?> clazz = null;
					try {
						clazz = Class.forName(component.getValue());
					} catch (ClassNotFoundException e) {
						m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Could not find class " + component.getValue() + " in runtime, component ignored.");
						continue;
					}
					
					if (!clazz.getSuperclass().equals(DXRAMComponent.class))
					{
						// check if there is an "interface"/abstract class between DXRAMComponent and the instance to create
						if (!clazz.getSuperclass().getSuperclass().equals(DXRAMComponent.class))
						{
							m_logger.error(DXRAM_ENGINE_LOG_HEADER, "DXRAMComponent is not a superclass of " + component.getValue() + ", component ignored.");
							continue;
						}
					}
					
					Constructor<?> ctor = null;
					
					try {
						ctor = clazz.getConstructor(Integer.TYPE, Integer.TYPE);
					} catch (NoSuchMethodException | SecurityException e1) {
						m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Could not get constructor of component " + component.getValue() + " invalid constructor, component ignored.");
						continue;
					}
					
					try {
						m_components.put(clazz.getName(), (DXRAMComponent) ctor.newInstance(new Object[] {priorityInit, priorityShutdown}));
					} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
						if (Modifier.isAbstract(clazz.getModifiers()))
							m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Component '" + component.getValue() + "' is an abstract class.");
						m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Could not create instance of component " + component.getValue() + ", component ignored.");
						continue;
					}
					
					m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Component " + component.getValue() + " enabled.");
				}
				else
				{
					m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Component " + component.getValue() + " disabled.");
				}
			}
		}
	}
	
	private void setupServices(final Configuration p_configuration)
	{
		Map<Integer, String> servicesClass = p_configuration.getValues("/DXRAMEngine/Services/Service/Class", String.class);
		Map<Integer, Boolean> servicesEnabled = p_configuration.getValues("/DXRAMEngine/Services/Service/Enabled", Boolean.class);
	
		for (Entry<Integer, String> service : servicesClass.entrySet())
		{
			Boolean enabled = servicesEnabled.get(service.getKey());
			if (enabled != null && enabled.booleanValue())
			{
				Class<?> clazz = null;
				try {
					clazz = Class.forName(service.getValue());
				} catch (ClassNotFoundException e) {
					m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Could not find class " + service.getValue() + " in runtime, service ignored.");
					continue;
				}
				
				if (!clazz.getSuperclass().equals(DXRAMService.class))
				{
					m_logger.error(DXRAM_ENGINE_LOG_HEADER, "DXRAMService is not a superclass of " + service.getValue() + ", service ignored.");
					continue;
				}
				
				try {
					m_services.put(clazz.getName(), (DXRAMService) clazz.newInstance());
				} catch (InstantiationException | IllegalAccessException e) {
					m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Could not create instance of service " + service.getValue() + ", service ignored.");
					continue;
				}
				
				m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Service " + service.getValue() + " enabled.");
			}
			else
			{
				m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Service " + service.getValue() + " disabled.");
			}
		}	
	}
	
	private void bootstrap(final String p_configurationFile, final String p_overrideNetworkIP, 
			final String p_overridePort, final String p_overrideRole)
	{
		m_configuration = new Configuration("DXRAMEngine");
			
		// overriding order:
		// config
		int configLoadSuccessful = loadConfiguration(p_configurationFile);
		
		// default configuration values from engine (if values don't exist)
		m_settings = new Settings(m_configuration, m_logger);
		registerDefaultConfigurationValues();
		
		setupLogger();
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Bootstrapping with configuration file: " + p_configurationFile);
		if (configLoadSuccessful != 0) {
			m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Loading from configuration file: " + configLoadSuccessful);
		}
		
		// parameters
		overrideConfigurationWithParameters(p_overrideNetworkIP, p_overridePort, p_overrideRole);
		
		// vm arguments
		overrideConfigurationWithVMArguments();
		
		m_logger.debug(DXRAM_ENGINE_LOG_HEADER, m_configuration.toString());
		
		// setup components and services
		setupComponents(m_configuration);
		setupServices(m_configuration);
		
		// if loading the configuration failed (file missing), write back the created version
		if (configLoadSuccessful == 1) {
			saveConfiguration(p_configurationFile);
		}
	}
	
	private void registerDefaultConfigurationValues() {
		m_settings.setDefaultValue(DXRAMEngineConfigurationValues.IP);
		m_settings.setDefaultValue(DXRAMEngineConfigurationValues.PORT);
		m_settings.setDefaultValue(DXRAMEngineConfigurationValues.ROLE);
		m_settings.setDefaultValue(DXRAMEngineConfigurationValues.LOGGER);
		m_settings.setDefaultValue(DXRAMEngineConfigurationValues.LOG_LEVEL);
		m_settings.setDefaultValue(DXRAMEngineConfigurationValues.JNI_LOCK_PATH);
	}
	
	private int loadConfiguration(final String p_configurationFile)
	{
		ConfigurationXMLLoader loader = new ConfigurationXMLLoaderFile(p_configurationFile);
		ConfigurationParser parser = new ConfigurationXMLParser(loader);
		
		//m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Loading configuration: " + loader);
		try {
			parser.readConfiguration(m_configuration);
		} catch (ConfigurationException e) {
			//m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Loading configuration failed.", e);
			// check if file exists -> save default config later
			if (!(new File(p_configurationFile).exists())) {
				//m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Creating default configuration file: " + configPath);
				return 1;
			} else {
				return -1;
			}
		}
		
		return 0;
	}
	
	private boolean saveConfiguration(final String p_configurationFolder)
	{
		ConfigurationXMLLoader loader = new ConfigurationXMLLoaderFile(p_configurationFolder);
		ConfigurationParser parser = new ConfigurationXMLParser(loader);
		
		m_logger.info(DXRAM_ENGINE_LOG_HEADER, "Saving configuration: " + loader);
		try {
			parser.writeConfiguration(m_configuration);
		} catch (ConfigurationException e) {
			m_logger.error(DXRAM_ENGINE_LOG_HEADER, "Writing configuration file '" + loader + "' failed.", e);
			return false;
		}
		
		return true;
	}
	
	private void overrideConfigurationWithParameters(final String p_networkIP, 
			final String p_port, final String p_role) {
		if (p_networkIP != null) {
			m_settings.overrideValue(DXRAMEngineConfigurationValues.IP, p_networkIP);
		}
		if (p_port != null) {
			m_settings.overrideValue(DXRAMEngineConfigurationValues.PORT, Integer.parseInt(p_port));
		}
		if (p_role != null) {
			m_settings.overrideValue(DXRAMEngineConfigurationValues.ROLE, p_role);
		}
	}
	
	private void overrideConfigurationWithVMArguments()
	{
		String[] keyValue;

		keyValue = new String[2];
		keyValue[0] = "dxram.network.ip";
		keyValue[1] = System.getProperty(keyValue[0]);
		if (keyValue[1] != null) {
			m_logger.debug(DXRAM_ENGINE_LOG_HEADER, "Overriding '" + keyValue[0] + "' with vm argument '" + keyValue[1] + "'.");
			m_settings.overrideValue(DXRAMEngineConfigurationValues.IP, keyValue[1]);
		}

		keyValue[0] = "dxram.network.port";
		keyValue[1] = System.getProperty(keyValue[0]);
		if (keyValue[1] != null) {
			m_logger.debug(DXRAM_ENGINE_LOG_HEADER, "Overriding '" + keyValue[0] + "' with vm argument '" + keyValue[1] + "'.");
			m_settings.overrideValue(DXRAMEngineConfigurationValues.PORT, Integer.parseInt(keyValue[1]));
		}

		keyValue[0] = "dxram.role";
		keyValue[1] = System.getProperty(keyValue[0]);
		if (keyValue[1] != null) {
			m_logger.debug(DXRAM_ENGINE_LOG_HEADER, "Overriding '" + keyValue[0] + "' with vm argument '" + keyValue[1] + "'.");
			m_settings.overrideValue(DXRAMEngineConfigurationValues.ROLE, keyValue[1]);
		}
	}
	
	private void setupLogger()
	{
		String logger = m_settings.getValue(DXRAMEngineConfigurationValues.LOGGER);
		String logLevel = m_settings.getValue(DXRAMEngineConfigurationValues.LOG_LEVEL);
		
		Class<?> clazz = null;
		try {
			clazz = Class.forName(logger);
		} catch (ClassNotFoundException e) {
			throw new DXRAMRuntimeException("Could not find class " + logger + " to create logger instance.");
		}
		
		boolean implementsInterface = false;
		for (Class<?> interfaces : clazz.getInterfaces())
		{
			if (interfaces.equals(Logger.class)) {
				implementsInterface = true;
				break;
			}
		}
		
		if (!implementsInterface)
			throw new DXRAMRuntimeException(logger + " does not implement interface DXRAMLogger.");
		
		Constructor<?> ctor = null;
		
		try {
			ctor = clazz.getConstructor();
		} catch (NoSuchMethodException | SecurityException e1) {
			throw new DXRAMRuntimeException("Could not get default constructor of logger " + logger + ".", e1);
		}
		
		try {
			m_logger = (Logger) ctor.newInstance();
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new DXRAMRuntimeException("Could not create instance of logger " + logger + ".", e);
		}
		
		m_logger.setLogLevel(LogLevel.toLogLevel(logLevel));
	}
	
	private void setupJNI()
	{
		m_logger.debug(DXRAM_ENGINE_LOG_HEADER, "Setting up JNI classes..." );
		
		String jniLock = m_settings.getValue(DXRAMEngineConfigurationValues.JNI_LOCK_PATH);
		JNILock.load(jniLock);
	}
}
