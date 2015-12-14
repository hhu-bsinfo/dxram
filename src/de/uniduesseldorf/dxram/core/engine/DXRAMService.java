package de.uniduesseldorf.dxram.core.engine;

import de.uniduesseldorf.utils.config.Configuration;

public abstract class DXRAMService 
{
	private DXRAMEngine m_parentEngine;
	
	private String m_serviceName;
	
	public DXRAMService(final String p_name)
	{
		m_serviceName = p_name;
	}
	
	public String getServiceName()
	{
		return m_serviceName;
	}
	
	public boolean start(final DXRAMEngine p_engine)
	{
		boolean ret = false;
		
		m_parentEngine = p_engine;
		
		m_parentEngine.getLogger().info("Starting service '" + m_serviceName + "'...");
		ret = startService(m_parentEngine.getConfiguration());
		if (ret == false)
        	m_parentEngine.getLogger().warn("Starting service '" + m_serviceName + "' failed.");
        else
        	m_parentEngine.getLogger().info("Starting service '" + m_serviceName + "'' successful.");
		
		return ret;
	}
	
	public boolean shutdown()
	{
		boolean ret = false;
	   
		m_parentEngine.getLogger().info("Shutting down service '" + m_serviceName + "'...");
        ret = shutdownService();
        if (ret == false)
        	m_parentEngine.getLogger().warn("Shutting down service '" + m_serviceName + "' failed.");
        else
        	m_parentEngine.getLogger().info("Shutting down service '" + m_serviceName + "' successful.");

        return ret;	
	}
	
	protected DXRAMSystemData getSystemData()
	{
		return m_parentEngine.getSystemData();
	}
	
	@SuppressWarnings("unchecked")
	protected <T extends DXRAMComponent> T getComponent(final String p_componentName)
	{		   
		return (T) m_parentEngine.getComponent(p_componentName);
	}
	
	protected abstract boolean startService(final Configuration p_configuration);
	
	protected abstract boolean shutdownService();
}
