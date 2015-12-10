package de.uniduesseldorf.dxram.core.engine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.engine.config.DXRAMConfigurationConstants;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfiguration;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfigurationFileParser;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesWatcher;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfigurationException;

import de.uniduesseldorf.utils.config.Configuration;
import de.uniduesseldorf.utils.config.ConfigurationFileParser;
import de.uniduesseldorf.utils.config.ConfigurationParser;
import de.uniduesseldorf.utils.config.Configuration.ConfigurationEntry;
import de.uniduesseldorf.utils.config.ConfigurationException;

public class DXRAMEngine 
{
	private static final String DXRAM_CONF_FILE = "dxram.conf";
	private static final String DXRAM_NODES_CONF_FILE = "nodes.conf";
	
	private final Logger m_logger = Logger.getLogger(DXRAMEngine.class);
	
	private boolean m_isInitilized;
	
	private DXRAMComponentSetupHandler m_componentSetupHandler;
	
	private Configuration m_configuration;
	private NodesWatcher m_nodesWatcher;
	private DXRAMSystemData m_systemData;
	
	private HashMap<String, DXRAMComponent> m_components = new HashMap<String, DXRAMComponent>();
	private HashMap<String, DXRAMService> m_services = new HashMap<String, DXRAMService>();
	
	public DXRAMEngine(final DXRAMComponentSetupHandler p_componentSetupHandler)
	{
		m_componentSetupHandler = p_componentSetupHandler;
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
		DXRAMComponent component = null;
		
		if (m_isInitilized)
		{
			component = m_components.get(p_identifier);
		}
		
		return component;
	}
	
	Configuration getConfiguration()
	{
		if (m_isInitilized)
			return m_configuration;
		else
			return null;
	}
	
	NodesConfiguration getNodesConfiguration()
	{
		if (m_isInitilized)
			return m_nodesWatcher.getNodesConfiguration();
		else
			return null;
	}
	
	DXRAMSystemData getSystemData()
	{
		if (m_isInitilized)
			return m_systemData;
		else
			return null;
	}
	
	Logger getLogger()
	{
		return m_logger;
	}
	
	public boolean init(final String p_configurationFolder) throws ConfigurationException, NodesConfigurationException
	{
		assert !m_isInitilized;
		
		List<DXRAMComponent> list;
		Comparator<DXRAMComponent> comp;
		
		m_logger.info("Initializing engine...");
		
		bootstrap(p_configurationFolder);
		
		if (m_componentSetupHandler != null)
		{
			m_logger.debug("Setting up components with external handler...");
			m_componentSetupHandler.setupComponents(m_configuration);
		}
		
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
	
	private void bootstrap(final String p_configurationFolder) throws ConfigurationException, NodesConfigurationException
	{
		String configurationFolder = p_configurationFolder;
		
		// normalize folder path
		if (configurationFolder.endsWith("/") && configurationFolder.length() > 1)
			configurationFolder.substring(0, configurationFolder.length() - 1);
		
		m_logger.info("Bootstrapping with configuration folder: " + configurationFolder);
		
		loadConfiguration(configurationFolder);
		overrideConfigurationWithVMArguments();
		
		setupNodesRouting(configurationFolder);
		
		initializeSystemData();
	}
	
	private void loadConfiguration(final String p_configurationFolder) throws ConfigurationException
	{
		File file;
		ConfigurationParser parser;
		
		file = new File(p_configurationFolder + "/" + DXRAM_CONF_FILE);
		parser = new ConfigurationFileParser(file);
		m_configuration = new Configuration();
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
			
			// create configuration with defined default values
			for (ConfigurationEntry<?> entry : DXRAMConfigurationConstants.getConfigurationEntries()) {
				m_configuration.setValue(entry.getKey(), entry.getDefaultValue().toString());
			}
			
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
		
		m_nodesWatcher = new NodesWatcher(m_configuration.getStringValue(DXRAMConfigurationConstants.NETWORK_IP), 
				m_configuration.getIntValue(DXRAMConfigurationConstants.NETWORK_PORT), 
				m_configuration.getStringValue(DXRAMConfigurationConstants.ZOOKEEPER_PATH), 
				m_configuration.getIntValue(DXRAMConfigurationConstants.ZOOKEEPER_BITFIELD_SIZE));
		
		m_nodesWatcher.setupNodeRouting(new NodesConfigurationFileParser(file));
	}
	
	private void initializeSystemData()
	{
		m_systemData = new DXRAMSystemData(m_nodesWatcher);
	}
	
	private void overrideConfigurationWithVMArguments()
	{
		String[] keyValue;

		keyValue = new String[2];
		keyValue[0] = "network.ip";
		keyValue[1] = System.getProperty(keyValue[0]);
		if (keyValue[1] != null) {
			m_logger.debug("Overriding '" + keyValue[0] + "' with vm argument '" + keyValue[1] + "'.");
			m_configuration.setValue(keyValue[0], keyValue[1]);
		}

		keyValue[0] = "network.port";
		keyValue[1] = System.getProperty(keyValue[0]);
		if (keyValue[1] != null) {
			m_logger.debug("Overriding '" + keyValue[0] + "' with vm argument '" + keyValue[1] + "'.");
			m_configuration.setValue(keyValue[0], keyValue[1]);
		}

		keyValue[0] = "dxram.role";
		keyValue[1] = System.getProperty(keyValue[0]);
		if (keyValue[1] != null) {
			m_logger.debug("Overriding '" + keyValue[0] + "' with vm argument '" + keyValue[1] + "'.");
			m_configuration.setValue(keyValue[0], keyValue[1]);
		}
	}
	
	private void closeNodesRouting()
	{
		// TODO last node has to set this to true ->
		// get information from superpeer overlay
		m_nodesWatcher.close(false);
	}
}
