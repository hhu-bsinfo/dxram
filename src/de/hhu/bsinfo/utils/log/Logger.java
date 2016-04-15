
package de.hhu.bsinfo.utils.log;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import de.hhu.bsinfo.utils.Pair;

/**
 * Implementation of a simple logger for error, warning and debug messages.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
 */
public class Logger implements LoggerInterface {
	private ArrayList<Pair<LogDestination, LogLevel>> m_logDestinations =
			new ArrayList<Pair<LogDestination, LogLevel>>();
	private LogLevel m_logLevel = LogLevel.DEBUG;
	private long m_timeMsStarted;

	/**
	 * Constructor
	 */
	public Logger() {
		m_timeMsStarted = System.currentTimeMillis();
	}

	/**
	 * Add a log destination to the logger. The log messages will be sent to
	 * every registered destination.
	 * @param p_dest
	 *            Destination to be added.
	 * @param p_logLevel
	 *            Initial log level to set for the destination
	 */
	public void addLogDestination(final LogDestination p_dest, final LogLevel p_logLevel) {
		m_logDestinations.add(new Pair<LogDestination, LogLevel>(p_dest, p_logLevel));
		if (m_logLevel.ordinal() > LogLevel.DISABLED.ordinal()) {
			if (p_logLevel.ordinal() > LogLevel.DISABLED.ordinal()) {
				p_dest.log(LogLevel.INFO, "***** Log started: "
						+ (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())).toString() + " *****");
			}
		}
	}

	/**
	 * Remove a already registered destination from the logger.
	 * @param p_dest
	 *            Destination to remove.
	 * @return True if removed, false if not found.
	 */
	public boolean removeLogDestination(final LogDestination p_dest) {
		if (m_logDestinations.remove(p_dest)) {
			return true;
		}

		return false;
	}

	/**
	 * Properly shut down the logger and have all registered log destinations shut down as well.
	 * This also clears all log destinations.
	 */
	public void close() {
		m_logDestinations.clear();
	}

	@Override
	public void setLogLevel(final LogLevel p_logLevel) {
		m_logLevel = p_logLevel;
	}

	@Override
	public void error(final String p_header, final String p_msg) {
		if (m_logLevel.ordinal() >= LogLevel.ERROR.ordinal()) {
			String str = createLogString(LogLevel.ERROR, p_header, p_msg, null);
			for (Pair<LogDestination, LogLevel> dest : m_logDestinations) {
				if (dest.m_second.ordinal() >= LogLevel.ERROR.ordinal()) {
					dest.first().log(LogLevel.ERROR, str);
				}
			}
		}
	}

	@Override
	public void error(final String p_header, final String p_msg, final Exception p_exception) {
		if (m_logLevel.ordinal() >= LogLevel.ERROR.ordinal()) {
			String str = createLogString(LogLevel.ERROR, p_header, p_msg, p_exception);
			for (Pair<LogDestination, LogLevel> dest : m_logDestinations) {
				if (dest.m_second.ordinal() >= LogLevel.ERROR.ordinal()) {
					dest.first().log(LogLevel.ERROR, str);
				}
			}
		}
	}

	@Override
	public void warn(final String p_header, final String p_msg) {
		if (m_logLevel.ordinal() >= LogLevel.WARN.ordinal()) {
			String str = createLogString(LogLevel.WARN, p_header, p_msg, null);
			for (Pair<LogDestination, LogLevel> dest : m_logDestinations) {
				if (dest.m_second.ordinal() >= LogLevel.WARN.ordinal()) {
					dest.first().log(LogLevel.WARN, str);
				}
			}
		}
	}

	@Override
	public void warn(final String p_header, final String p_msg, final Exception p_exception) {
		if (m_logLevel.ordinal() >= LogLevel.WARN.ordinal()) {
			String str = createLogString(LogLevel.WARN, p_header, p_msg, p_exception);
			for (Pair<LogDestination, LogLevel> dest : m_logDestinations) {
				if (dest.m_second.ordinal() >= LogLevel.WARN.ordinal()) {
					dest.first().log(LogLevel.WARN, str);
				}
			}
		}
	}

	@Override
	public void info(final String p_header, final String p_msg) {
		if (m_logLevel.ordinal() >= LogLevel.INFO.ordinal()) {
			String str = createLogString(LogLevel.INFO, p_header, p_msg, null);
			for (Pair<LogDestination, LogLevel> dest : m_logDestinations) {
				if (dest.m_second.ordinal() >= LogLevel.INFO.ordinal()) {
					dest.first().log(LogLevel.INFO, str);
				}
			}
		}
	}

	@Override
	public void info(final String p_header, final String p_msg, final Exception p_exception) {
		if (m_logLevel.ordinal() >= LogLevel.INFO.ordinal()) {
			String str = createLogString(LogLevel.INFO, p_header, p_msg, p_exception);
			for (Pair<LogDestination, LogLevel> dest : m_logDestinations) {
				if (dest.m_second.ordinal() >= LogLevel.INFO.ordinal()) {
					dest.first().log(LogLevel.INFO, str);
				}
			}
		}
	}

	@Override
	public void debug(final String p_header, final String p_msg) {
		if (m_logLevel.ordinal() >= LogLevel.DEBUG.ordinal()) {
			String str = createLogString(LogLevel.DEBUG, p_header, p_msg, null);
			for (Pair<LogDestination, LogLevel> dest : m_logDestinations) {
				if (dest.m_second.ordinal() >= LogLevel.DEBUG.ordinal()) {
					dest.first().log(LogLevel.DEBUG, str);
				}
			}
		}
	}

	@Override
	public void debug(final String p_header, final String p_msg, final Exception p_exception) {
		if (m_logLevel.ordinal() >= LogLevel.DEBUG.ordinal()) {
			String str = createLogString(LogLevel.DEBUG, p_header, p_msg, p_exception);
			for (Pair<LogDestination, LogLevel> dest : m_logDestinations) {
				if (dest.m_second.ordinal() >= LogLevel.DEBUG.ordinal()) {
					dest.first().log(LogLevel.DEBUG, str);
				}
			}
		}
	}

	@Override
	public void trace(final String p_header, final String p_msg) {
		if (m_logLevel.ordinal() >= LogLevel.TRACE.ordinal()) {
			String str = createLogString(LogLevel.TRACE, p_header, p_msg, null);
			for (Pair<LogDestination, LogLevel> dest : m_logDestinations) {
				if (dest.m_second.ordinal() >= LogLevel.TRACE.ordinal()) {
					dest.first().log(LogLevel.TRACE, str);
				}
			}
		}
	}

	@Override
	public void trace(final String p_header, final String p_msg, final Exception p_exception) {
		if (m_logLevel.ordinal() >= LogLevel.TRACE.ordinal()) {
			String str = createLogString(LogLevel.TRACE, p_header, p_msg, p_exception);
			for (Pair<LogDestination, LogLevel> dest : m_logDestinations) {
				if (dest.m_second.ordinal() >= LogLevel.TRACE.ordinal()) {
					dest.first().log(LogLevel.TRACE, str);
				}
			}
		}
	}

	/**
	 * Create the log string to write to the destinations
	 * @param p_logLevel
	 *            Log level for the message
	 * @param p_header
	 *            Log message header
	 * @param p_msg
	 *            Log message
	 * @param p_exception
	 *            Optional exception
	 * @return Log string
	 */
	private String createLogString(final LogLevel p_logLevel, final String p_header, final String p_msg,
			final Exception p_exception) {
		String str = new String();

		str += "[" + (System.currentTimeMillis() - m_timeMsStarted) + "]";
		str += "[" + p_logLevel.toString() + "]";
		str += "[TID: " + Thread.currentThread().getId() + "]";
		str += "[" + p_header + "] ";
		str += p_msg;
		if (p_exception != null) {
			str += "\n##########\n#### " + p_exception + "\n";
			for (StackTraceElement ste : p_exception.getStackTrace()) {
				str += "#### " + ste + "\n";
			}
			str += "##########";
		}
		return str;
	}
}
