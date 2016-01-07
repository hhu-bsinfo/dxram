package de.uniduesseldorf.dxram.core.engine;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfiguration;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfigurationFileParser;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesWatcher;
import de.uniduesseldorf.dxram.core.net.NetworkConfigurationValues;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfigurationException;
import de.uniduesseldorf.utils.Pair;
import de.uniduesseldorf.utils.conf.Configuration;
import de.uniduesseldorf.utils.conf.ConfigurationException;
import de.uniduesseldorf.utils.conf.ConfigurationParser;
import de.uniduesseldorf.utils.conf.ConfigurationXMLLoader;
import de.uniduesseldorf.utils.conf.ConfigurationXMLLoaderFile;
import de.uniduesseldorf.utils.conf.ConfigurationXMLParser;

public class DXRAMEngine 
{
	private static final String DXRAM_CONF_FILE = "dxram.config";
	
	private final Logger m_logger = Logger.getLogger(DXRAMEngine.class);
	
	public static class Settings
	{
		private Configuration m_configuration = null;
		private String m_basePath = new String();
		
		Settings(final Configuration p_configuration)
		{
			m_configuration = p_configuration;
			m_basePath = "/DXRAMEngine/Settings/";
		}
		
		public <T> void overrideValue(final Pair<String, T> p_default, final T p_overrideValue)
		{
			m_configuration.AddValue(m_basePath + p_default.first(), p_overrideValue, true);
		}
		
		public <T> void setDefaultValue(final Pair<String, T> p_default)
		{
			setDefaultValue(p_default.first(), p_default.second());
		}
		
		public <T> void setDefaultValue(final String p_key, final T p_value)
		{
			m_configuration.AddValue(m_basePath + p_key, p_value, false);
		}
		
		@SuppressWarnings("unchecked")
		public <T> T getValue(final Pair<String, T> p_default)
		{
			return (T) getValue(p_default.first(), p_default.second().getClass());
		}
		
		public <T> T getValue(final String p_key, final Class<T> p_type)
		{
			try {
				return m_configuration.GetValue(m_basePath + p_key, p_type);
			} catch (ConfigurationException e) {
				throw new DXRAMRuntimeException(e.getMessage());
			}
		}
	}
	
	private boolean m_isInitilized;
	
	private Configuration m_configuration;
	private Settings m_settings;
	
	private HashMap<String, DXRAMComponent> m_components = new HashMap<String, DXRAMComponent>();
	private HashMap<String, DXRAMService> m_services = new HashMap<String, DXRAMService>();
	
	public DXRAMEngine()
	{		
		// TODO have this setable through config/console arguments?
		Logger.getRootLogger().setLevel(Level.TRACE);
	}
	
	public <T extends DXRAMService> T getService(final Class<T> p_class)
	{
		T service = null;
		
		if (m_isInitilized) {
			DXRAMService tmpService = m_services.get(p_class.getName());
			if (tmpService != null && p_class.isInstance(tmpService)) {
				service = p_class.cast(tmpService);
			}
		} 
		
		return service;
	}
	
	<T extends DXRAMComponent> T getComponent(final Class<T> p_class)
	{
		T component = null;
	
		DXRAMComponent tmpComponent = m_components.get(p_class.getName());
		if (tmpComponent != null && p_class.isInstance(tmpComponent)) {
			component = p_class.cast(tmpComponent);
		}
		
		return component;
	}
	
	Configuration getConfiguration()
	{
		return m_configuration;
	}
	
	Logger getLogger()
	{
		return m_logger;
	}
	
	public boolean init(final String p_configurationFolder) {
		return init(p_configurationFolder, null, null, null);
	}
	
	public boolean init(final String p_configurationFolder, final String p_overrideNetworkIP, 
			final String p_overridePort, final String p_overrideRole)
	{
		assert !m_isInitilized;
		
		List<DXRAMComponent> list;
		Comparator<DXRAMComponent> comp;
		
		m_logger.info("Initializing engine...");

		bootstrap(p_configurationFolder, p_overrideNetworkIP, 
				p_overridePort, p_overrideRole);
		
		m_logger.debug("Setting up components...");
		setupComponents(m_configuration);
		m_logger.debug("Setting up services...");
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
        
        m_logger.info("Initializing " + list.size() + " components...");
        for (DXRAMComponent component : list) {
            if (component.init(this) == false) {
            	m_logger.error("Initializing component '" + component.getIdentifier() + "' failed, aborting init.");
                return false;
            }
        }
        m_logger.info("Initializing components done.");
        
        m_logger.info("Starting " + m_services.size() + " services...");
        for (DXRAMService service : m_services.values()) {
            if (service.start(this) == false) {
            	m_logger.error("Starting service '" + service.getServiceName() + "' failed, aborting init.");
                return false;
            }
        }
        m_logger.info("Starting services done.");

        m_logger.info("Initializing engine done.");
        m_isInitilized = true;
		
		return true;
	}
	
	public boolean shutdown()
	{
		assert m_isInitilized;

		List<DXRAMComponent> list;
		Comparator<DXRAMComponent> comp;
		
		m_logger.info("Shutting down engine...");

		m_logger.info("Shutting down " + m_services.size() + " services...");
        for (DXRAMService service : m_services.values()) {
            if (service.shutdown() == false) {
            	m_logger.error("Shutting down service '" + service.getServiceName() + "' failed.");
            }
        }
        m_logger.info("Shutting down services done.");
		
		list = new ArrayList<DXRAMComponent>(m_components.values());

	    comp = new Comparator<DXRAMComponent>() {
            @Override
            public int compare(DXRAMComponent o1, DXRAMComponent o2) {
                return (new Integer(o1.getPriorityShutdown())).compareTo(new Integer(o2.getPriorityShutdown()));
            }
        };

        Collections.sort(list, comp);
        
        m_logger.info("Shutting down " + list.size() + " components...");

        for (DXRAMComponent component : list) {
        	component.shutdown();
        }
        
        m_logger.info("Shutting down components done.");
        
        closeNodesRouting();

        m_logger.info("Shutting down engine done.");

        m_isInitilized = false;

		return true;
	}
	
	private void setupComponents(final Configuration p_configuration)
	{
		Map<Integer, String> componentsClass = p_configuration.GetValues("/DXRAMEngine/Components/Component/Class", String.class);
		Map<Integer, Boolean> componentsEnabled = p_configuration.GetValues("/DXRAMEngine/Components/Component/Enabled", Boolean.class);
		Map<Integer, Integer> componentsPriorityInit = p_configuration.GetValues("/DXRAMEngine/Components/Component/PriorityInit", Integer.class);
		Map<Integer, Integer> componentsPriorityUninit = p_configuration.GetValues("/DXRAMEngine/Components/Component/PriorityUninit", Integer.class);
		
		for (Entry<Integer, String> component : componentsClass.entrySet())
		{
			Boolean enabled = componentsEnabled.get(component.getKey());
			if (enabled != null && enabled.booleanValue())
			{
				Integer priorityInit = componentsPriorityInit.get(component.getValue());
				if (priorityInit == null)
				{
					m_logger.error("Cannot setup component " + component.getValue() + " missing init priority, component ignored.");
					continue;
				}
				
				Integer priorityUninit = componentsPriorityUninit.get(component.getValue());
				if (priorityUninit == null)
				{
					m_logger.error("Cannot setup component " + component.getValue() + " missing uninit priority, component ignored.");
					continue;
				}
				
				Class<?> clazz = null;
				try {
					clazz = Class.forName(component.getValue());
				} catch (ClassNotFoundException e) {
					m_logger.error("Could not find class " + component.getValue() + " in runtime, component ignored.");
					continue;
				}
				
				if (!clazz.getSuperclass().equals(DXRAMComponent.class))
				{
					m_logger.error("DXRAMComponent is not a superclass of " + component.getValue() + ", component ignored.");
					continue;
				}
				
				Constructor<?> ctor = null;
				
				try {
					ctor = clazz.getConstructor(Integer.class, Integer.class);
				} catch (NoSuchMethodException | SecurityException e1) {
					m_logger.error("Could not create instance of component " + component.getValue() + " invalid constructor, component ignored.");
					continue;
				}
				
				try {
					m_components.put(clazz.getName(), (DXRAMComponent) ctor.newInstance(new Object[] {priorityInit, priorityUninit}));
				} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
					m_logger.error("Could not create instance of component " + component.getValue() + ", component ignored.");
					continue;
				}
				
				m_logger.info("Component " + component.getValue() + " enabled.");
			}
			else
			{
				m_logger.info("Component " + component.getValue() + " disabled.");
			}
		}
	}
	
	private void setupServices(final Configuration p_configuration)
	{
		Map<Integer, String> servicesClass = p_configuration.GetValues("/DXRAMEngine/Services/Service/Class", String.class);
		Map<Integer, Boolean> servicesEnabled = p_configuration.GetValues("/DXRAMEngine/Services/Service/Enabled", Boolean.class);
	
		for (Entry<Integer, String> service : servicesClass.entrySet())
		{
			Boolean enabled = servicesEnabled.get(service.getKey());
			if (enabled != null && enabled.booleanValue())
			{
				Class<?> clazz = null;
				try {
					clazz = Class.forName(service.getValue());
				} catch (ClassNotFoundException e) {
					m_logger.error("Could not find class " + service.getValue() + " in runtime, service ignored.");
					continue;
				}
				
				if (!clazz.getSuperclass().equals(DXRAMService.class))
				{
					m_logger.error("DXRAMService is not a superclass of " + service.getValue() + ", service ignored.");
					continue;
				}
				
				try {
					m_services.put(clazz.getName(), (DXRAMService) clazz.newInstance());
				} catch (InstantiationException | IllegalAccessException e) {
					m_logger.error("Could not create instance of service " + service.getValue() + ", service ignored.");
					continue;
				}
				
				m_logger.info("Service " + service.getValue() + " enabled.");
			}
			else
			{
				m_logger.info("Service " + service.getValue() + " disabled.");
			}
		}	
	}
	
	private void bootstrap(final String p_configurationFolder, final String p_overrideNetworkIP, 
			final String p_overridePort, final String p_overrideRole)
	{
		String configurationFolder = p_configurationFolder;
		
		// normalize folder path
		if (configurationFolder.endsWith("/") && configurationFolder.length() > 1)
			configurationFolder.substring(0, configurationFolder.length() - 1);
		
		m_logger.info("Bootstrapping with configuration folder: " + configurationFolder);
		
		m_configuration = new Configuration("DXRAMEngine");
		
		// overriding order:
		// config
		boolean configLoadSuccessful = loadConfiguration(configurationFolder);
		
		// parameters
		overrideConfigurationWithParameters(p_overrideNetworkIP, p_overridePort, p_overrideRole);
		
		// vm arguments
		overrideConfigurationWithVMArguments();
		
		// default configuration values from engine (if values don't exist)
		m_settings = new Settings(m_configuration);
		registerDefaultConfigurationValues();
		
		// setup components and services
		setupComponents(m_configuration);
		setupServices(m_configuration);
		
		// if loading the configuration failed (file missing), write back the created version
		if (configLoadSuccessful) {
			saveConfiguration(configurationFolder);
		}
	}
	
	private void registerDefaultConfigurationValues() {
		m_settings.setDefaultValue(DXRAMEngineConfigurationValues.IP);
		m_settings.setDefaultValue(DXRAMEngineConfigurationValues.PORT);
		m_settings.setDefaultValue(DXRAMEngineConfigurationValues.ROLE);
	}
	
	private boolean loadConfiguration(final String p_configurationFolder)
	{
		ConfigurationXMLLoader loader = new ConfigurationXMLLoaderFile(p_configurationFolder + "/" + DXRAM_CONF_FILE);
		ConfigurationParser parser = new ConfigurationXMLParser(loader);
		
		// TODO try to laod from file -> if file not available, set flag to memorize this
		// and continue with empty configuration -> components and services fill them with their default values before
		// initialization
		// write the configuration file to disk before init of components and services but after all default values are set
		
		m_logger.info("Loading configuration: " + loader);
		parser.readConfiguration(m_configuration);
		
		//m_logger.warn("Configuration file '" + file + "' does not exist, creating default.");

		return true;
	}
	
	private boolean saveConfiguration(final String p_configurationFolder)
	{
		ConfigurationXMLLoader loader = new ConfigurationXMLLoaderFile(p_configurationFolder + "/" + DXRAM_CONF_FILE);
		ConfigurationParser parser = new ConfigurationXMLParser(loader);
		
		m_logger.info("Saving configuration: " + loader);
		parser.writeConfiguration(m_configuration);
		
		// TODO catch errors if writing failed
		
		return true;
	}
	
	private void overrideConfigurationWithParameters(final String p_networkIP, 
			final String p_port, final String p_role) {
		if (p_networkIP != null) {
			m_settings.overrideValue(DXRAMEngineConfigurationValues.IP, p_networkIP);
		}
		if (p_port != null) {
			m_settings.overrideValue(DXRAMEngineConfigurationValues.PORT, Short.parseShort(p_port));
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
			m_logger.debug("Overriding '" + keyValue[0] + "' with vm argument '" + keyValue[1] + "'.");
			m_settings.overrideValue(DXRAMEngineConfigurationValues.IP, keyValue[1]);
		}

		keyValue[0] = "dxram.network.port";
		keyValue[1] = System.getProperty(keyValue[0]);
		if (keyValue[1] != null) {
			m_logger.debug("Overriding '" + keyValue[0] + "' with vm argument '" + keyValue[1] + "'.");
			m_settings.overrideValue(DXRAMEngineConfigurationValues.PORT, Short.parseShort(keyValue[1]));
		}

		keyValue[0] = "dxram.role";
		keyValue[1] = System.getProperty(keyValue[0]);
		if (keyValue[1] != null) {
			m_logger.debug("Overriding '" + keyValue[0] + "' with vm argument '" + keyValue[1] + "'.");
			m_settings.overrideValue(DXRAMEngineConfigurationValues.ROLE, keyValue[1]);
		}
	}
}
