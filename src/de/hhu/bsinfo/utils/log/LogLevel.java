
package de.hhu.bsinfo.utils.log;

/**
 * List of different log levels available for logging.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public enum LogLevel {
	// 0
	DISABLED("disabled"),
	// 1
	ERROR("error"),
	// 2
	WARN("warn"),
	// 3
	INFO("info"),
	// 4
	DEBUG("debug"),
	// 5
	TRACE("trace");

	private final String m_name;

	/**
	 * Constructor
	 * @param p_str
	 *            String representation of the log level.
	 */
	LogLevel(final String p_str) {
		m_name = p_str;
	}

	/**
	 * Convert a string representing the log level to an enum item.
	 * @param p_string
	 *            Log level as string.
	 * @return Enumeration item matching the string or disabled if string does not match any log level.
	 */
	public static LogLevel toLogLevel(final String p_string) {
		if (p_string == null) {
			return LogLevel.DISABLED;
		}

		String str = p_string.toLowerCase();

		switch (str) {
		case "error":
			return LogLevel.ERROR;
		case "warning":
		case "warn":
			return LogLevel.WARN;
		case "info":
			return LogLevel.INFO;
		case "debug":
			return LogLevel.DEBUG;
		case "trace":
			return LogLevel.TRACE;
		default:
			return LogLevel.DISABLED;
		}
	}

	/**
	 * Compare this log level with another one represented as a string.
	 * @param p_str
	 *            String to compare to.
	 * @return True if this log level matches the string representation, false otherwise.
	 */
	boolean equalsStr(final String p_str) {
		return (p_str == null) ? false : m_name.equals(p_str);
	}

	@Override
	public String toString() {
		return m_name;
	}
}
