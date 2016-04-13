
package de.hhu.bsinfo.dxram.logger;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.logger.tcmds.TcmdChangeLogLevel;
import de.hhu.bsinfo.dxram.term.TerminalComponent;
import de.hhu.bsinfo.utils.log.LogLevel;

/**
 * Service to allow the application to use the same logger as DXRAM.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 02.02.16
 */
public class LoggerService extends AbstractDXRAMService {

	private LoggerComponent m_logger;
	private TerminalComponent m_terminal;

	/**
	 * Log an error message.
	 * @param <T>
	 *            Type of the class
	 * @param p_class
	 *            Class calling this method.
	 * @param p_msg
	 *            Message to log.
	 */
	public <T> void error(final Class<T> p_class, final String p_msg) {
		m_logger.error(getClass(), "[" + p_class.getSimpleName() + "] " + p_msg);
	}

	/**
	 * Log an error message.
	 * @param <T>
	 *            Type of the class
	 * @param p_class
	 *            Class calling this method.
	 * @param p_msg
	 *            Message to log.
	 * @param p_e
	 *            Exception to add to the log message.
	 */
	public <T> void error(final Class<T> p_class, final String p_msg, final Exception p_e) {
		m_logger.error(getClass(), "[" + p_class.getSimpleName() + "] " + p_msg);
	}

	/**
	 * Log a warning message.
	 * @param <T>
	 *            Type of the class
	 * @param p_class
	 *            Class calling this method.
	 * @param p_msg
	 *            Message to log.
	 */
	public <T> void warn(final Class<T> p_class, final String p_msg) {
		m_logger.warn(getClass(), "[" + p_class.getSimpleName() + "] " + p_msg);
	}

	/**
	 * Log a warning message.
	 * @param <T>
	 *            Type of the class
	 * @param p_class
	 *            Class calling this method.
	 * @param p_msg
	 *            Message to log.
	 * @param p_e
	 *            Exception to add to the log message.
	 */
	public <T> void warn(final Class<T> p_class, final String p_msg, final Exception p_e) {
		m_logger.warn(getClass(), "[" + p_class.getSimpleName() + "] " + p_msg);
	}

	/**
	 * Log an info message.
	 * @param <T>
	 *            Type of the class
	 * @param p_class
	 *            Class calling this method.
	 * @param p_msg
	 *            Message to log.
	 */
	public <T> void info(final Class<T> p_class, final String p_msg) {
		m_logger.info(getClass(), "[" + p_class.getSimpleName() + "] " + p_msg);
	}

	/**
	 * Log an info message.
	 * @param <T>
	 *            Type of the class
	 * @param p_class
	 *            Class calling this method.
	 * @param p_msg
	 *            Message to log.
	 * @param p_e
	 *            Exception to add to the log message.
	 */
	public <T> void info(final Class<T> p_class, final String p_msg, final Exception p_e) {
		m_logger.info(getClass(), "[" + p_class.getSimpleName() + "] " + p_msg);
	}

	/**
	 * Log a debug message.
	 * @param <T>
	 *            Type of the class
	 * @param p_class
	 *            Class calling this method.
	 * @param p_msg
	 *            Message to log.
	 */
	public <T> void debug(final Class<T> p_class, final String p_msg) {
		m_logger.debug(getClass(), "[" + p_class.getSimpleName() + "] " + p_msg);
	}

	/**
	 * Log a debug message.
	 * @param <T>
	 *            Type of the class
	 * @param p_class
	 *            Class calling this method.
	 * @param p_msg
	 *            Message to log.
	 * @param p_e
	 *            Exception to add to the log message.
	 */
	public <T> void debug(final Class<T> p_class, final String p_msg, final Exception p_e) {
		m_logger.debug(getClass(), "[" + p_class.getSimpleName() + "] " + p_msg);
	}

	/**
	 * Log a trace message.
	 * @param <T>
	 *            Type of the class
	 * @param p_class
	 *            Class calling this method.
	 * @param p_msg
	 *            Message to log.
	 */
	public <T> void trace(final Class<T> p_class, final String p_msg) {
		m_logger.trace(getClass(), "[" + p_class.getSimpleName() + "] " + p_msg);
	}

	/**
	 * Log a trace message.
	 * @param <T>
	 *            Type of the class
	 * @param p_class
	 *            Class calling this method.
	 * @param p_msg
	 *            Message to log.
	 * @param p_e
	 *            Exception to add to the log message.
	 */
	public <T> void trace(final Class<T> p_class, final String p_msg, final Exception p_e) {
		m_logger.trace(getClass(), "[" + p_class.getSimpleName() + "] " + p_msg);
	}

	/**
	 * Set the log level for the logger.
	 * @param p_logLevel
	 *            Log level to set.
	 */
	public void setLogLevel(final String p_logLevel) {
		LogLevel level = LogLevel.toLogLevel(p_logLevel);
		m_logger.setLogLevel(level);
	}

	@Override
	protected void registerDefaultSettingsService(final Settings p_settings) {

	}

	@Override
	protected boolean startService(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {
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
