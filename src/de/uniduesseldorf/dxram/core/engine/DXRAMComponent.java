package de.uniduesseldorf.dxram.core.engine;

import java.lang.reflect.Modifier;
import java.util.Map;

import de.uniduesseldorf.dxram.core.util.logger.Logger;

import de.uniduesseldorf.utils.Pair;
import de.uniduesseldorf.utils.conf.Configuration;

public abstract class DXRAMComponent 
{
	public static class Settings
	{
		private static final String CONFIG_ROOT = "/DXRAMEngine/ComponentSettings/";
		
		private Configuration m_configuration = null;
		private Logger m_logger = null;
		private String m_commonBasePath = new String();
		private String m_basePath = new String();
		
		Settings(final Configuration p_configuration, final Logger p_logger, final String p_componentInterfaceIdentifier, final String p_componentImplementationIdentifier)
		{
			m_configuration = p_configuration;
			m_logger = p_logger;
			m_commonBasePath = CONFIG_ROOT;
			if (!p_componentInterfaceIdentifier.isEmpty())
				m_commonBasePath += p_componentInterfaceIdentifier + "/";
			m_basePath = m_commonBasePath + p_componentImplementationIdentifier + "/";
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
				m_logger.warn(this.getClass().getSimpleName(), "Settings value for '" + p_key + "' was missing, using default value '" + p_value + "'.");
			}
		}
		
		@SuppressWarnings("unchecked")
		public <T> T getValue(final Pair<String, T> p_default)
		{
			return (T) getValue(p_default.first(), p_default.second().getClass());
		}
		
		public <T> T getValue(final String p_key, final Class<T> p_type)
		{
			// try implementation specific path first, then common interface path
			T val = m_configuration.GetValue(m_basePath + p_key, p_type);
			if (val == null)
				val = m_configuration.GetValue(m_commonBasePath + p_key, p_type);
			
			return val;
		}
		
		public <T> Map<Integer, T> GetValues(final String p_key, final Class<T> p_type)
		{
			// try implementation specific path first, then common interface path
			Map<Integer, T> vals = m_configuration.GetValues(m_basePath + p_key, p_type);
			if (vals == null || vals.isEmpty())
				vals = m_configuration.GetValues(m_commonBasePath + p_key, p_type);
			
			return vals;
		}
	}
	
	private DXRAMEngine m_parentEngine;
	private Settings m_settings;
	
	private int m_priorityInit;
	private int m_priorityShutdown;
	
	public DXRAMComponent(final int p_priorityInit, final int p_priorityShutdown)
	{
		m_priorityInit = p_priorityInit;
		m_priorityShutdown = p_priorityShutdown;
	}
	
	public String getComponentName()
	{
		return this.getClass().getSimpleName();
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

		String componentInterfaceIdentifier = new String();
		// is superclass abstract and not DXRAMComponent -> interface
		if (Modifier.isAbstract(this.getClass().getSuperclass().getModifiers()) &&
				!this.getClass().getSuperclass().equals(DXRAMComponent.class))
		{
			componentInterfaceIdentifier = this.getClass().getSuperclass().getName();
		}
		
		m_settings = new Settings(	m_parentEngine.getConfiguration(), 
				m_parentEngine.getLogger(),
				componentInterfaceIdentifier, 
				this.getClass().getName());
		
		registerDefaultSettingsComponent(m_settings);
		
		m_parentEngine.getLogger().info(this.getClass().getSimpleName(), "Initializing component...");
        ret = initComponent(m_parentEngine.getSettings(), m_settings);
        if (ret == false)
        	m_parentEngine.getLogger().error(this.getClass().getSimpleName(), "Initializing component failed.");
        else
        	m_parentEngine.getLogger().info(this.getClass().getSimpleName(), "Initializing component successful.");

        return ret;
	}
	
   public boolean shutdown() {
	   boolean ret = false;
	   
	   m_parentEngine.getLogger().info(this.getClass().getSimpleName(), "Shutting down component...");
        ret = shutdownComponent();
        if (ret == false)
        	m_parentEngine.getLogger().warn(this.getClass().getSimpleName(), "Shutting down component failed.");
        else
        	m_parentEngine.getLogger().info(this.getClass().getSimpleName(), "Shutting down component successful.");

        return ret;
    }
   
   // ------------------------------------------------------------------------------
   
   protected <T extends DXRAMComponent> T getDependantComponent(final Class<T> p_class)
   {		   	   
	   return m_parentEngine.getComponent(p_class);
   }
   
   protected Logger getLogger()
   {
	   return m_parentEngine.getLogger();
   }
   
   protected DXRAMEngine getParentEngine()
   {
	  return m_parentEngine; 
   }
   
   protected abstract void registerDefaultSettingsComponent(final Settings p_settings);
   
   protected abstract boolean initComponent(final DXRAMEngine.Settings p_engineSettings, final Settings p_settings);
   
   protected abstract boolean shutdownComponent();
}
