package de.hhu.bsinfo.dxram.util.logger;

public enum LogLevel
{
	DISABLED("disabled"), // 0
	ERROR("error"), // 1
	WARN("warn"), // 2
	INFO("info"), // 3
	DEBUG("debug"), // 4
	TRACE("trace"); // 5
	
	private final String m_name;
	
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
	
	private LogLevel(final String p_str)
	{
		m_name = p_str;
	}
	
	public boolean equals(final String str)
	{
		return (str == null) ? false : m_name.equals(str);
	}
	
	public String toString() {
		return m_name;
	}
}