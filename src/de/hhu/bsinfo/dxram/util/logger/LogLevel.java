package de.hhu.bsinfo.dxram.util.logger;

/**
 * List of different log levels available for logging.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public enum LogLevel
{
	DISABLED("disabled"), // 0
	ERROR("error"), // 1
	WARN("warn"), // 2
	INFO("info"), // 3
	DEBUG("debug"), // 4
	TRACE("trace"); // 5
	
	private final String m_name;
	
	/**
	 * Convert a string representing the log level to an enum item.
	 * @param p_string Log level as string.
	 * @return Enumeration item matching the string or disabled if string does not match any log level.
	 */
	public static LogLevel toLogLevel(final String p_string)
	{
		if (p_string == null)
			return LogLevel.DISABLED;
		
		String str = p_string.toLowerCase();
		
		switch (str)
		{
			case "error": return LogLevel.ERROR;
			case "warning":
			case "warn": return LogLevel.WARN;
			case "info": return LogLevel.INFO;
			case "debug": return LogLevel.DEBUG;
			case "trace": return LogLevel.TRACE;
			default:
				return LogLevel.DISABLED;
		}
	}
	
	/**
	 * Constructor
	 * @param p_str String representation of the log level.
	 */
	private LogLevel(final String p_str)
	{
		m_name = p_str;
	}
	
	/**
	 * Compare this log level with another one represented as a string.
	 * @param str String to compare to.
	 * @return True if this log level matches the string representation, false otherwise.
	 */
	public boolean equals(final String str)
	{
		return (str == null) ? false : m_name.equals(str);
	}
	
	@Override
	public String toString() {
		return m_name;
	}
}