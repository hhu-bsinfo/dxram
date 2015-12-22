package de.uniduesseldorf.dxram.core.engine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfiguration;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfigurationFileParser;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesWatcher;
import de.uniduesseldorf.dxram.core.net.NetworkConfigurationValues;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfigurationException;

import de.uniduesseldorf.utils.config.Configuration;
import de.uniduesseldorf.utils.config.ConfigurationFileParser;
import de.uniduesseldorf.utils.config.ConfigurationParser;
import de.uniduesseldorf.utils.config.ConfigurationException;

public class DXRAMEngine 
{
	private static final String DXRAM_CONF_FILE = "dxram.config";
	private static final String DXRAM_NODES_CONF_FILE = "nodes.config";
	
	private final Logger m_logger = Logger.getLogger(DXRAMEngine.class);
	
	private boolean m_isInitilized;
	
	private DXRAMEngineSetupHandler m_setupHandler;
	
	private Configuration m_configuration;
	private NodesWatcher m_nodesWatcher;
	private DXRAMSystemData m_systemData;
	
	private HashMap<String, DXRAMComponent> m_components = new HashMap<String, DXRAMComponent>();
	private HashMap<String, DXRAMService> m_services = new HashMap<String, DXRAMService>();
	
	public DXRAMEngine(final DXRAMEngineSetupHandler p_setupHandler)
	{
		m_setupHandler = p_setupHandler;
		
		// TODO have this setable through config/console arguments?
		Logger.getRootLogger().setLevel(Level.TRACE);
	}
	
	public boolean addComponent(final DXRAMComponent p_component)
	{
		boolean ret = false;
		
		if (!m_isInitilized)
		{
			// no duplicated components
			if (!m_components.containsKey(p_component.getIdentifier())) {
				m_components.put(p_component.getIdentifier(), p_component);
				ret = true;
			}
			else
			{
				m_logger.error("Component '" + p_component.getIdentifier() + "' is already registered.");
			}
		}
			
		return ret;
	}
	
	public boolean addService(final DXRAMService p_service)
	{
		boolean ret = false;
		
		if (!m_isInitilized)
		{
			// no duplicated components
			if (!m_services.containsKey(p_service.getServiceName())) {
				m_services.put(p_service.getServiceName(), p_service);
				ret = true;
			}
			else
			{
				m_logger.error("Service '" + p_service.getServiceName() + "' is already registered.");
			}
		}
			
		return ret;
	}
	
	public boolean clearComponents() {
		boolean ret = false;
		
		if (!m_isInitilized)
		{
			m_components.clear();
			ret = true;
		}
		
		return ret;
	}
	
	public boolean clearServices() {
		boolean ret = false;
		
		if (!m_isInitilized)
		{
			m_services.clear();
			ret = true;
		}
		
		return ret;
	}
	
	public DXRAMService getService(final String p_name)
	{
		DXRAMService service = null;
		
		if (m_isInitilized)
		{
			service = m_services.get(p_name);
		}
		
		return service;
	}
	
	DXRAMComponent getComponent(final String p_identifier)
	{
		return m_components.get(p_identifier);
	}
	
	Configuration getConfiguration()
	{
		return m_configuration;
	}
	
	NodesConfiguration getNodesConfiguration()
	{
		return m_nodesWatcher.getNodesConfiguration();
	}
	
	DXRAMSystemData getSystemData()
	{
		return m_systemData;
	}
	
	Logger getLogger()
	{
		return m_logger;
	}
	
	public boolean init(final String p_configurationFolder) throws ConfigurationException, NodesConfigurationException {
		return init(p_configurationFolder, null, null, null);
	}
	
	public boolean init(final String p_configurationFolder, final String p_overrideNetworkIP, 
			final String p_overridePort, final String p_overrideRole) 
					throws ConfigurationException, NodesConfigurationException
	{
		assert !m_isInitilized;
		
		List<DXRAMComponent> list;
		Comparator<DXRAMComponent> comp;
		
		m_logger.info("Initializing engine...");
		
		// allow external handler to register further components and/or services
		if (m_setupHandler != null) {
			m_logger.debug("Setting up components with external handler...");
			m_setupHandler.setupComponents(this, m_configuration);
			m_logger.debug("Setting up services with external handler...");
			m_setupHandler.setupServices(this, m_configuration);
		}
		
		bootstrap(p_configurationFolder, p_overrideNetworkIP, 
				p_overridePort, p_overrideRole);
		
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
	
	private void bootstrap(final String p_configurationFolder, final String p_overrideNetworkIP, 
			final String p_overridePort, final String p_overrideRole) 
					throws ConfigurationException, NodesConfigurationException
	{
		String configurationFolder = p_configurationFolder;
		
		// normalize folder path
		if (configurationFolder.endsWith("/") && configurationFolder.length() > 1)
			configurationFolder.substring(0, configurationFolder.length() - 1);
		
		m_logger.info("Bootstrapping with configuration folder: " + configurationFolder);
		
		m_configuration = new Configuration();
		
		registerConfigurationValues();
		
		// register configuration values of components
		for (DXRAMComponent component : m_components.values()) {
			component.registerConfigurationValues(m_configuration);
		}
		
		// overriding order:
		// config
		loadConfiguration(configurationFolder);
		
		// parameters
		overrideConfigurationWithParameters(p_overrideNetworkIP, p_overridePort, p_overrideRole);
		
		// vm arguments
		overrideConfigurationWithVMArguments();
		
		setupNodesRouting(configurationFolder);
		
		initializeSystemData();
	}
	
	private void registerConfigurationValues() {
		m_configuration.registerConfigurationEntries(DXRAMConfigurationValues.CONFIGURATION_ENTRIES);
	}
	
	private void loadConfiguration(final String p_configurationFolder) throws ConfigurationException
	{
		File file;
		ConfigurationParser parser;
		
		file = new File(p_configurationFolder + "/" + DXRAM_CONF_FILE);
		parser = new ConfigurationFileParser(file);
		if (file.exists())
		{
			m_logger.debug("Loading configuration file: " + file);
			
			parser.readConfiguration(m_configuration);
		}
		else
		{
			m_logger.warn("Configuration file '" + file + "' does not exist, creating default.");
			try {
				file.createNewFile();
			} catch (IOException e) {
				throw new DXRAMRuntimeException("Creating new file '" + file + "' failed.");
			}
			
			// default values already written with registering values
			
			// write back configuration
			parser.writeConfiguration(m_configuration);
		}
	}
	
	private void setupNodesRouting(final String p_configurationFolder) throws NodesConfigurationException
	{
		File file;
		
		file = new File(p_configurationFolder + "/" + DXRAM_NODES_CONF_FILE);
		if (!file.exists())
		{
			m_logger.warn("Nodes configuration file does not exist: " + file);
		}
		
		m_nodesWatcher = new NodesWatcher(m_configuration.getStringValue(NetworkConfigurationValues.NETWORK_IP), 
				m_configuration.getIntValue(NetworkConfigurationValues.NETWORK_PORT), 
				m_configuration.getStringValue(DXRAMConfigurationValues.ZOOKEEPER_PATH), 
				m_configuration.getStringValue(DXRAMConfigurationValues.ZOOKEEPER_CONNECTION_STRING),
				m_configuration.getIntValue(DXRAMConfigurationValues.ZOOKEEPER_TIMEOUT),
				m_configuration.getIntValue(DXRAMConfigurationValues.ZOOKEEPER_BITFIELD_SIZE));
		
		m_nodesWatcher.setupNodeRouting(new NodesConfigurationFileParser(file));
	}
	
	private void initializeSystemData()
	{
		m_systemData = new DXRAMSystemData(m_nodesWatcher);
	}
	
	private void overrideConfigurationWithParameters(final String p_networkIP, 
			final String p_port, final String p_role) {
		if (p_networkIP != null) {
			m_configuration.setValue(NetworkConfigurationValues.NETWORK_IP, p_networkIP);
		}
		if (p_port != null) {
			m_configuration.setValue(NetworkConfigurationValues.NETWORK_PORT, p_port);
		}
		if (p_role != null) {
			m_configuration.setValue(DXRAMConfigurationValues.DXRAM_ROLE, p_role);
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
			m_configuration.setValue(NetworkConfigurationValues.NETWORK_IP, keyValue[1]);
		}

		keyValue[0] = "dxram.network.port";
		keyValue[1] = System.getProperty(keyValue[0]);
		if (keyValue[1] != null) {
			m_logger.debug("Overriding '" + keyValue[0] + "' with vm argument '" + keyValue[1] + "'.");
			m_configuration.setValue(NetworkConfigurationValues.NETWORK_PORT, keyValue[1]);
		}

		keyValue[0] = "dxram.role";
		keyValue[1] = System.getProperty(keyValue[0]);
		if (keyValue[1] != null) {
			m_logger.debug("Overriding '" + keyValue[0] + "' with vm argument '" + keyValue[1] + "'.");
			m_configuration.setValue(DXRAMConfigurationValues.DXRAM_ROLE, keyValue[1]);
		}
	}
	
	private void closeNodesRouting()
	{
		// TODO last node has to set this to true ->
		// get information from superpeer overlay
		m_nodesWatcher.close(false);
	}
}
