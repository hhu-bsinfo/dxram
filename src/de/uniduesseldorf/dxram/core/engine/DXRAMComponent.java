package de.uniduesseldorf.dxram.core.engine;

import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfiguration;

import de.uniduesseldorf.utils.config.Configuration;

public abstract class DXRAMComponent 
{
	private DXRAMEngine m_parentEngine;
	
	private String m_identifier;
	
	private int m_priorityInit;
	
	private int m_priorityShutdown;
	
	public DXRAMComponent(final String p_componentIdentifier, final int p_priorityInit, final int p_priorityShutdown)
	{
		m_identifier = p_componentIdentifier;
		m_priorityInit = p_priorityInit;
		m_priorityShutdown = p_priorityShutdown;
	}
	
	public String getIdentifier()
	{
		return m_identifier;
	}
	
	public int getPriorityInit()
	{
		return m_priorityInit;
	}
	
	public int getPriorityShutdown()
	{
		return m_priorityShutdown;
	}
	
	public void registerConfigurationValues(final Configuration p_configuration) {
		registerConfigurationValuesComponent(p_configuration);
	}
	
	public boolean init(final DXRAMEngine p_engine)
	{
		boolean ret = false;
		
		m_parentEngine = p_engine;
		
		m_parentEngine.getLogger().info("Initializing component '" + m_identifier + "'...");
        ret = initComponent(m_parentEngine.getConfiguration());
        if (ret == false)
        	m_parentEngine.getLogger().warn("Initializing component '" + m_identifier + "' failed.");
        else
        	m_parentEngine.getLogger().info("Initializing component '" + m_identifier + "'' successful.");

        return ret;
	}
	
   public boolean shutdown() {
	   boolean ret = false;
	   
	   m_parentEngine.getLogger().info("Shutting down component '" + m_identifier + "'...");
        ret = shutdownComponent();
        if (ret == false)
        	m_parentEngine.getLogger().warn("Shutting down component '" + m_identifier + "' failed.");
        else
        	m_parentEngine.getLogger().info("Shutting down component '" + m_identifier + "' successful.");

        return ret;
    }
   
   // ------------------------------------------------------------------------------
   
   protected DXRAMSystemData getSystemData()
   {
	   return m_parentEngine.getSystemData();
   }
   
   @SuppressWarnings("unchecked")
   protected <T extends DXRAMComponent> T getDependantComponent(final String p_componentName)
   {		   
	   return (T) m_parentEngine.getComponent(p_componentName);
   }
   
   protected abstract void registerConfigurationValuesComponent(final Configuration p_configuration);
   
   protected abstract boolean initComponent(final Configuration p_configuration);
   
   protected abstract boolean shutdownComponent();
}
