package de.hhu.bsinfo.dxram.logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import de.hhu.bsinfo.dxram.engine.DXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.util.logger.LogLevel;

public class LoggerComponent extends DXRAMComponent
{
	private LogLevel m_defaultLogLevel = LogLevel.DISABLED;
	private Map<String, LogLevel> m_logLevels = new HashMap<String, LogLevel>();
	
	public LoggerComponent(int p_priorityInit, int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}
	
	// -------------------------------------------------------------------------------------
	
	public <T> void error(final Class<T> clazz, final String msg)
	{
		LogLevel logLevel = m_logLevels.get(clazz.getName());
		if (logLevel == null) {
			logLevel = m_defaultLogLevel;
		}
		
		if (LogLevel.ERROR.ordinal() <= logLevel.ordinal())
			getLogger().error(clazz.getName(), msg);
	}
	
	public <T> void error(final Class<T> clazz, final String msg, final Exception e)
	{
		LogLevel logLevel = m_logLevels.get(clazz.getName());
		if (logLevel == null) {
			logLevel = m_defaultLogLevel;
		}
		
		if (LogLevel.ERROR.ordinal() <= logLevel.ordinal())
			getLogger().error(clazz.getName(), msg, e);
	}
	
	public <T> void warn(final Class<T> clazz, final String msg)
	{
		LogLevel logLevel = m_logLevels.get(clazz.getName());
		if (logLevel == null) {
			logLevel = m_defaultLogLevel;
		}
		
		if (LogLevel.WARN.ordinal() <= logLevel.ordinal())
			getLogger().warn(clazz.getName(), msg);
	}
	
	public <T> void warn(final Class<T> clazz, final String msg, final Exception e)
	{
		LogLevel logLevel = m_logLevels.get(clazz.getName());
		if (logLevel == null) {
			logLevel = m_defaultLogLevel;
		}
		
		if (LogLevel.WARN.ordinal() <= logLevel.ordinal())
			getLogger().warn(clazz.getName(), msg, e);
	}
	
	public <T> void info(final Class<T> clazz, final String msg)
	{
		LogLevel logLevel = m_logLevels.get(clazz.getName());
		if (logLevel == null) {
			logLevel = m_defaultLogLevel;
		}
		
		if (LogLevel.INFO.ordinal() <= logLevel.ordinal())
			getLogger().info(clazz.getName(), msg);
	}
	
	public <T> void info(final Class<T> clazz, final String msg, final Exception e)
	{
		LogLevel logLevel = m_logLevels.get(clazz.getName());
		if (logLevel == null) {
			logLevel = m_defaultLogLevel;
		}
		
		if (LogLevel.INFO.ordinal() <= logLevel.ordinal())
			getLogger().info(clazz.getName(), msg, e);
	}
	
	public <T> void debug(final Class<T> clazz, final String msg)
	{
		LogLevel logLevel = m_logLevels.get(clazz.getName());
		if (logLevel == null) {
			logLevel = m_defaultLogLevel;
		}
		
		if (LogLevel.DEBUG.ordinal() <= logLevel.ordinal())
			getLogger().debug(clazz.getName(), msg);
	}
	
	public <T> void debug(final Class<T> clazz, final String msg, final Exception e)
	{
		LogLevel logLevel = m_logLevels.get(clazz.getName());
		if (logLevel == null) {
			logLevel = m_defaultLogLevel;
		}
		
		if (LogLevel.DEBUG.ordinal() <= logLevel.ordinal())
			getLogger().debug(clazz.getName(), msg, e);
	}
	
	public <T> void trace(final Class<T> clazz, final String msg)
	{
		LogLevel logLevel = m_logLevels.get(clazz.getName());
		if (logLevel == null) {
			logLevel = m_defaultLogLevel;
		}
		
		if (LogLevel.TRACE.ordinal() <= logLevel.ordinal())
			getLogger().trace(clazz.getName(), msg);
	}
	
	public <T> void trace(final Class<T> clazz, final String msg, final Exception e)
	{
		LogLevel logLevel = m_logLevels.get(clazz.getName());
		if (logLevel == null) {
			logLevel = m_defaultLogLevel;
		}
		
		if (LogLevel.TRACE.ordinal() <= logLevel.ordinal())
			getLogger().trace(clazz.getName(), msg, e);
	}
	
	// -------------------------------------------------------------------------------------

	@Override
	protected void registerDefaultSettingsComponent(Settings p_settings) {
		p_settings.setDefaultValue(LoggerConfigurationValues.Component.LOG_LEVEL);
	}

	@Override
	protected boolean initComponent(DXRAMEngine.Settings p_engineSettings,
			Settings p_settings) {		
		m_defaultLogLevel = LogLevel.toLogLevel(p_settings.getValue(LoggerConfigurationValues.Component.LOG_LEVEL));
		
		// get further configuration values for 
		Map<Integer, String> classNames = p_settings.GetValues("Class/Name", String.class);
		Map<Integer, String> logLevels = p_settings.GetValues("Class/LogLevel", String.class);
		
		if (classNames != null)
		{
			for (Entry<Integer, String> entries : classNames.entrySet())
			{
				LogLevel logLevel = LogLevel.toLogLevel(logLevels.get(entries.getKey()));
				m_logLevels.put(entries.getValue(), logLevel);	
			}
		}
		
		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		return true;
	}
}