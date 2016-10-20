
package de.hhu.bsinfo.dxram.logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.utils.logger.LogLevel;
import de.hhu.bsinfo.utils.logger.LoggerInterface;

/**
 * This component provides an extended interface for logging. It enables
 * distinguishing log messages of different classes/components/services thus
 * allowing filtering and redirecting of log messages.
 * This component should be used by all other components and services to do any
 * logging.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class LoggerComponent extends AbstractDXRAMComponent implements LoggerInterface {
	private Map<String, LogLevel> m_logLevels = new HashMap<String, LogLevel>();

	/**
	 * Constructor
	 * @param p_priorityInit
	 *            Priority for initialization of this component.
	 *            When choosing the order, consider component dependencies here.
	 * @param p_priorityShutdown
	 *            Priority for shutting down this component.
	 *            When choosing the order, consider component dependencies here.
	 */
	public LoggerComponent(final int p_priorityInit, final int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

	// -------------------------------------------------------------------------------------

	/**
	 * Log an error message.
	 * @param <T>
	 *            Type of the class
	 * @param p_clazz
	 *            Class calling this method.
	 * @param p_msg
	 *            Message to log.
	 */
	public <T> void error(final Class<T> p_clazz, final String p_msg) {
		LogLevel logLevel = m_logLevels.get(p_clazz.getSimpleName());
		if (logLevel == null) {
			logLevel = LogLevel.TRACE;
		}

		if (LogLevel.ERROR.ordinal() <= logLevel.ordinal()) {
			getLogger().error(p_clazz.getSimpleName(), p_msg);
		}
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
		LogLevel logLevel = m_logLevels.get(p_class.getSimpleName());
		if (logLevel == null) {
			logLevel = LogLevel.TRACE;
		}

		if (LogLevel.ERROR.ordinal() <= logLevel.ordinal()) {
			getLogger().error(p_class.getSimpleName(), p_msg, p_e);
		}
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
		LogLevel logLevel = m_logLevels.get(p_class.getSimpleName());
		if (logLevel == null) {
			logLevel = LogLevel.TRACE;
		}

		if (LogLevel.WARN.ordinal() <= logLevel.ordinal()) {
			getLogger().warn(p_class.getSimpleName(), p_msg);
		}
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
		LogLevel logLevel = m_logLevels.get(p_class.getSimpleName());
		if (logLevel == null) {
			logLevel = LogLevel.TRACE;
		}

		if (LogLevel.WARN.ordinal() <= logLevel.ordinal()) {
			getLogger().warn(p_class.getSimpleName(), p_msg, p_e);
		}
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
		LogLevel logLevel = m_logLevels.get(p_class.getSimpleName());
		if (logLevel == null) {
			logLevel = LogLevel.TRACE;
		}

		if (LogLevel.INFO.ordinal() <= logLevel.ordinal()) {
			getLogger().info(p_class.getSimpleName(), p_msg);
		}
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
		LogLevel logLevel = m_logLevels.get(p_class.getSimpleName());
		if (logLevel == null) {
			logLevel = LogLevel.TRACE;
		}

		if (LogLevel.INFO.ordinal() <= logLevel.ordinal()) {
			getLogger().info(p_class.getSimpleName(), p_msg, p_e);
		}
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
		LogLevel logLevel = m_logLevels.get(p_class.getSimpleName());
		if (logLevel == null) {
			logLevel = LogLevel.TRACE;
		}

		if (LogLevel.DEBUG.ordinal() <= logLevel.ordinal()) {
			getLogger().debug(p_class.getSimpleName(), p_msg);
		}
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
		LogLevel logLevel = m_logLevels.get(p_class.getSimpleName());
		if (logLevel == null) {
			logLevel = LogLevel.TRACE;
		}

		if (LogLevel.DEBUG.ordinal() <= logLevel.ordinal()) {
			getLogger().debug(p_class.getSimpleName(), p_msg, p_e);
		}
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
		LogLevel logLevel = m_logLevels.get(p_class.getSimpleName());
		if (logLevel == null) {
			logLevel = LogLevel.TRACE;
		}

		if (LogLevel.TRACE.ordinal() <= logLevel.ordinal()) {
			getLogger().trace(p_class.getSimpleName(), p_msg);
		}
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
		LogLevel logLevel = m_logLevels.get(p_class.getSimpleName());
		if (logLevel == null) {
			logLevel = LogLevel.TRACE;
		}

		if (LogLevel.TRACE.ordinal() <= logLevel.ordinal()) {
			getLogger().trace(p_class.getSimpleName(), p_msg, p_e);
		}
	}

	// -------------------------------------------------------------------------------------

	@Override
	protected void registerDefaultSettingsComponent(final Settings p_settings) {

	}

	@Override
	protected boolean initComponent(final DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {

		// get further configuration values for
		Map<Integer, String> classNames = p_settings.getValues("Class/Name", String.class);
		Map<Integer, String> logLevels = p_settings.getValues("Class/LogLevel", String.class);

		if (classNames != null) {
			for (Entry<Integer, String> entries : classNames.entrySet()) {
				LogLevel logLevel = LogLevel.toLogLevel(logLevels.get(entries.getKey()));
				m_logLevels.put(entries.getValue(), logLevel);
			}
		}

		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		m_logLevels.clear();
		return true;
	}

	@Override
	public void setLogLevel(final LogLevel p_logLevel) {
		getLogger().setLogLevel(p_logLevel);
	}

	@Override
	public void error(final String p_header, final String p_msg) {
		LogLevel logLevel = m_logLevels.get(p_header);
		if (logLevel == null) {
			logLevel = LogLevel.TRACE;
		}

		if (LogLevel.ERROR.ordinal() <= logLevel.ordinal()) {
			getLogger().error(p_header, p_msg);
		}
	}

	@Override
	public void error(final String p_header, final String p_msg, final Exception p_e) {
		LogLevel logLevel = m_logLevels.get(p_header);
		if (logLevel == null) {
			logLevel = LogLevel.TRACE;
		}

		if (LogLevel.ERROR.ordinal() <= logLevel.ordinal()) {
			getLogger().error(p_header, p_msg, p_e);
		}
	}

	@Override
	public void warn(final String p_header, final String p_msg) {
		LogLevel logLevel = m_logLevels.get(p_header);
		if (logLevel == null) {
			logLevel = LogLevel.TRACE;
		}

		if (LogLevel.WARN.ordinal() <= logLevel.ordinal()) {
			getLogger().warn(p_header, p_msg);
		}
	}

	@Override
	public void warn(final String p_header, final String p_msg, final Exception p_e) {
		LogLevel logLevel = m_logLevels.get(p_header);
		if (logLevel == null) {
			logLevel = LogLevel.TRACE;
		}

		if (LogLevel.WARN.ordinal() <= logLevel.ordinal()) {
			getLogger().warn(p_header, p_msg, p_e);
		}
	}

	@Override
	public void info(final String p_header, final String p_msg) {
		LogLevel logLevel = m_logLevels.get(p_header);
		if (logLevel == null) {
			logLevel = LogLevel.TRACE;
		}

		if (LogLevel.INFO.ordinal() <= logLevel.ordinal()) {
			getLogger().info(p_header, p_msg);
		}
	}

	@Override
	public void info(final String p_header, final String p_msg, final Exception p_e) {
		LogLevel logLevel = m_logLevels.get(p_header);
		if (logLevel == null) {
			logLevel = LogLevel.TRACE;
		}

		if (LogLevel.INFO.ordinal() <= logLevel.ordinal()) {
			getLogger().info(p_header, p_msg, p_e);
		}
	}

	@Override
	public void debug(final String p_header, final String p_msg) {
		LogLevel logLevel = m_logLevels.get(p_header);
		if (logLevel == null) {
			logLevel = LogLevel.TRACE;
		}

		if (LogLevel.DEBUG.ordinal() <= logLevel.ordinal()) {
			getLogger().debug(p_header, p_msg);
		}
	}

	@Override
	public void debug(final String p_header, final String p_msg, final Exception p_e) {
		LogLevel logLevel = m_logLevels.get(p_header);
		if (logLevel == null) {
			logLevel = LogLevel.TRACE;
		}

		if (LogLevel.DEBUG.ordinal() <= logLevel.ordinal()) {
			getLogger().debug(p_header, p_msg, p_e);
		}
	}

	@Override
	public void trace(final String p_header, final String p_msg) {
		LogLevel logLevel = m_logLevels.get(p_header);
		if (logLevel == null) {
			logLevel = LogLevel.TRACE;
		}

		if (LogLevel.TRACE.ordinal() <= logLevel.ordinal()) {
			getLogger().trace(p_header, p_msg);
		}
	}

	@Override
	public void trace(final String p_header, final String p_msg, final Exception p_e) {
		LogLevel logLevel = m_logLevels.get(p_header);
		if (logLevel == null) {
			logLevel = LogLevel.TRACE;
		}

		if (LogLevel.TRACE.ordinal() <= logLevel.ordinal()) {
			getLogger().trace(p_header, p_msg, p_e);
		}
	}
}
