package de.uniduesseldorf.dxram.core.engine;

import de.uniduesseldorf.utils.Pair;
import de.uniduesseldorf.utils.conf.Configuration;
import de.uniduesseldorf.utils.conf.ConfigurationException;

public abstract class DXRAMComponent 
{
	public static class Settings
	{
		private Configuration m_configuration = null;
		private String m_basePath = new String();
		
		Settings(final Configuration p_configuration, final String p_componentIdentifier)
		{
			m_configuration = p_configuration;
			m_basePath = "/DXRAMEngine/ComponentSettings/" + p_componentIdentifier + "/";
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
	
	private DXRAMEngine m_parentEngine;
	private Settings m_settings;
	
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
	
	public boolean init(final DXRAMEngine p_engine)
	{
		boolean ret = false;
		
		m_parentEngine = p_engine;
		m_settings = new Settings(m_parentEngine.getConfiguration(), m_identifier);
		registerDefaultSettingsComponent(m_settings);
		
		m_parentEngine.getLogger().info("Initializing component '" + m_identifier + "'...");
        ret = initComponent(m_settings);
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
   
   protected abstract void registerDefaultSettingsComponent(final Settings p_settings);
   
   protected abstract boolean initComponent(final Settings p_settings);
   
   protected abstract boolean shutdownComponent();
}
