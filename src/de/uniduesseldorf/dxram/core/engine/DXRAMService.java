package de.uniduesseldorf.dxram.core.engine;

import de.uniduesseldorf.dxram.core.util.logger.Logger;

import de.uniduesseldorf.utils.Pair;
import de.uniduesseldorf.utils.conf.Configuration;

public abstract class DXRAMService 
{
	public static class Settings
	{
		private Configuration m_configuration = null;
		private Logger m_logger = null;
		private String m_basePath = new String();
		
		Settings(final Configuration p_configuration, final Logger p_logger, final String p_serviceIdentifier)
		{
			m_configuration = p_configuration;
			m_logger = p_logger;
			m_basePath = "/DXRAMEngine/ServiceSettings/" + p_serviceIdentifier + "/";
		}
		
		public <T> void setDefaultValue(final Pair<String, T> p_default)
		{
			setDefaultValue(p_default.first(), p_default.second());
		}
		
		public <T> void setDefaultValue(final String p_key, final T p_value)
		{
			if (m_configuration.AddValue(m_basePath + p_key, p_value, false))
			{
				// we added a default value => value was missing from configuration
				m_logger.warn(this.getClass().getSimpleName(), "Settings value for '" + p_key + "' was missing, using default value " + p_value);
			}
		}
		
		@SuppressWarnings("unchecked")
		public <T> T getValue(final Pair<String, T> p_default)
		{
			return (T) getValue(p_default.first(), p_default.second().getClass());
		}
		
		public <T> T getValue(final String p_key, final Class<T> p_type)
		{
			return m_configuration.GetValue(m_basePath + p_key, p_type);
		}
	}
	
	private DXRAMEngine m_parentEngine = null;
	private Settings m_settings = null;
	
	public DXRAMService()
	{
		
	}
	
	public String getServiceName()
	{
		return this.getClass().getSimpleName();
	}
	
	public boolean start(final DXRAMEngine p_engine)
	{
		boolean ret = false;
		
		m_parentEngine = p_engine;
		m_settings = new Settings(m_parentEngine.getConfiguration(), m_parentEngine.getLogger(), getServiceName());
		
		m_parentEngine.getLogger().info(this.getClass().getSimpleName(), "Starting service...");
		ret = startService(m_parentEngine.getSettings(), m_settings);
		if (ret == false)
			m_parentEngine.getLogger().error(this.getClass().getSimpleName(), "Starting service failed.");
        else
        	m_parentEngine.getLogger().info(this.getClass().getSimpleName(), "Starting service successful.");
		
		return ret;
	}
	
	public boolean shutdown()
	{
		boolean ret = false;
	   
		m_parentEngine.getLogger().info(this.getClass().getSimpleName(), "Shutting down service...");
        ret = shutdownService();
        if (ret == false)
        	m_parentEngine.getLogger().warn(this.getClass().getSimpleName(), "Shutting down service failed.");
        else
        	m_parentEngine.getLogger().info(this.getClass().getSimpleName(), "Shutting down service successful.");

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
