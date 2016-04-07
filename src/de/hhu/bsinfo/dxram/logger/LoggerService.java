package de.hhu.bsinfo.dxram.logger;

import de.hhu.bsinfo.dxram.engine.DXRAMService;
import de.hhu.bsinfo.dxram.logger.tcmds.TcmdChangeLogLevel;
import de.hhu.bsinfo.dxram.term.TerminalComponent;
import de.hhu.bsinfo.utils.log.LogLevel;

/**
 * Service to allow the application to use the same logger as DXRAM.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 02.02.16
 */
public class LoggerService extends DXRAMService {

	private LoggerComponent m_logger = null;
	private TerminalComponent m_terminal = null;
	
	/**
	 * Log an error message.
	 * @param clazz Class calling this method.
	 * @param msg Message to log.
	 */
	public <T> void error(final Class<T> clazz, final String msg)
	{
		m_logger.error(getClass(), "[" + clazz.getSimpleName() + "] " + msg);
	}
	
	/**
	 * Log an error message.
	 * @param clazz Class calling this method.
	 * @param msg Message to log.
	 * @param e Exception to add to the log message.
	 */
	public <T> void error(final Class<T> clazz, final String msg, final Exception e)
	{
		m_logger.error(getClass(), "[" + clazz.getSimpleName() + "] " + msg);
	}
	
	/**
	 * Log a warning message.
	 * @param clazz Class calling this method.
	 * @param msg Message to log.
	 */
	public <T> void warn(final Class<T> clazz, final String msg)
	{
		m_logger.warn(getClass(), "[" + clazz.getSimpleName() + "] " + msg);
	}
	
	/**
	 * Log a warning message.
	 * @param clazz Class calling this method.
	 * @param msg Message to log.
	 * @param e Exception to add to the log message.
	 */
	public <T> void warn(final Class<T> clazz, final String msg, final Exception e)
	{
		m_logger.warn(getClass(), "[" + clazz.getSimpleName() + "] " + msg);
	}
	
	/**
	 * Log an info message.
	 * @param clazz Class calling this method.
	 * @param msg Message to log.
	 */
	public <T> void info(final Class<T> clazz, final String msg)
	{
		m_logger.info(getClass(), "[" + clazz.getSimpleName() + "] " + msg);
	}
	
	/**
	 * Log an info message.
	 * @param clazz Class calling this method.
	 * @param msg Message to log.
	 * @param e Exception to add to the log message.
	 */
	public <T> void info(final Class<T> clazz, final String msg, final Exception e)
	{
		m_logger.info(getClass(), "[" + clazz.getSimpleName() + "] " + msg);
	}
	
	/**
	 * Log a debug message.
	 * @param clazz Class calling this method.
	 * @param msg Message to log.
	 */
	public <T> void debug(final Class<T> clazz, final String msg)
	{
		m_logger.debug(getClass(), "[" + clazz.getSimpleName() + "] " + msg);
	}
	
	/**
	 * Log a debug message.
	 * @param clazz Class calling this method.
	 * @param msg Message to log.
	 * @param e Exception to add to the log message.
	 */
	public <T> void debug(final Class<T> clazz, final String msg, final Exception e)
	{
		m_logger.debug(getClass(), "[" + clazz.getSimpleName() + "] " + msg);
	}
	
	/**
	 * Log a trace message.
	 * @param clazz Class calling this method.
	 * @param msg Message to log.
	 */
	public <T> void trace(final Class<T> clazz, final String msg)
	{
		m_logger.trace(getClass(), "[" + clazz.getSimpleName() + "] " + msg);
	}
	
	/**
	 * Log a trace message.
	 * @param clazz Class calling this method.
	 * @param msg Message to log.
	 * @param e Exception to add to the log message.
	 */
	public <T> void trace(final Class<T> clazz, final String msg, final Exception e)
	{
		m_logger.trace(getClass(), "[" + clazz.getSimpleName() + "] " + msg);
	}
	
	public void setLogLevel(String p_logLevel)
	{
		LogLevel level = LogLevel.toLogLevel(p_logLevel);
		m_logger.setLogLevel(level);
	}
	
	@Override
	protected void registerDefaultSettingsService(Settings p_settings) {

	}

	@Override
	protected boolean startService(de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			Settings p_settings) {
		m_logger = getComponent(LoggerComponent.class);
		
		m_terminal = getComponent(TerminalComponent.class);
		m_terminal.registerCommand(new TcmdChangeLogLevel());
		
		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_logger = null;
		
		return true;
	}

}
