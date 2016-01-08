package de.uniduesseldorf.dxram.core.engine;

import de.uniduesseldorf.dxram.core.engine.DXRAMComponent.Settings;
import de.uniduesseldorf.utils.Pair;
import de.uniduesseldorf.utils.conf.Configuration;
import de.uniduesseldorf.utils.conf.ConfigurationException;

public abstract class DXRAMService 
{
	public static class Settings
	{
		private Configuration m_configuration = null;
		private String m_basePath = new String();
		
		Settings(final Configuration p_configuration, final String p_serviceIdentifier)
		{
			m_configuration = p_configuration;
			m_basePath = "/DXRAMEngine/ServiceSettings/" + p_serviceIdentifier + "/";
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
		m_settings = new Settings(m_parentEngine.getConfiguration(), m_serviceName);
		
		m_parentEngine.getLogger().info("Starting service '" + m_serviceName + "'...");
		ret = startService(m_settings);
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
	
	protected <T extends DXRAMComponent> T getComponent(final Class<T> p_class)
	{		   
		   return m_parentEngine.getComponent(p_class);
	}
	
	protected abstract void registerDefaultSettingsService(final Settings p_settings);
	
	protected abstract boolean startService(final DXRAMEngine.Settings p_engineSettings, final Settings p_settings);
	
	protected abstract boolean shutdownService();
}
