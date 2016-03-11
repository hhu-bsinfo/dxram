package de.hhu.bsinfo.utils.log;

import java.util.ArrayList;

/**
 * Implementation of a simple logger for error, warning and debug messages.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
 */
public class Logger implements LoggerInterface
{
	private ArrayList<LogDestination> m_logDestinations = new ArrayList<LogDestination>();
	private LogLevel m_logLevel = LogLevel.DEBUG;
	
	/**
	 * Add a log destination to the logger. The log messages will be sent to 
	 * every registered destination.
	 * @param dest Destination to be added.
	 */
	public void addLogDestination(LogDestination dest)
	{ 
		m_logDestinations.add(dest); 
		if (m_logLevel.ordinal() > LogLevel.DISABLED.ordinal()) {
			dest.logStart();
		}
	}
	
	/**
	 * Remove a already registered destination from the logger.
	 * @param dest Destination to remove.
	 * @return True if removed, false if not found.
	 */
	public boolean removeLogDestination(LogDestination dest)
	{
	    if (m_logDestinations.remove(dest))
	    {
	        dest.logEnd();
	        return true;
	    }
	    
	    return false;
	}
	
	/**
	 * Properly shut down the logger and have all registered log destinations shut down as well.
	 * This also clears all log destinations.
	 */
	public void close()
	{
		for (LogDestination dest : m_logDestinations) {
			if (m_logLevel.ordinal() > LogLevel.DISABLED.ordinal()) {
				dest.logEnd();
			}
		}
			
		m_logDestinations.clear();		
	}
	
	@Override
	public void setLogLevel(LogLevel p_logLevel) {
		m_logLevel = p_logLevel;
	}

	@Override
	public void error(String p_header, String p_msg) {
		if (m_logLevel.ordinal() >= LogLevel.ERROR.ordinal())
		{
			for (LogDestination dest : m_logDestinations)
				dest.log(LogLevel.ERROR, p_header, p_msg, null);
		}
	}

	@Override
	public void error(String p_header, String p_msg, Exception p_exception) {
		if (m_logLevel.ordinal() >= LogLevel.ERROR.ordinal())
		{
			for (LogDestination dest : m_logDestinations)
				dest.log(LogLevel.ERROR, p_header, p_msg, p_exception);
		}
	}

	@Override
	public void warn(String p_header, String p_msg) {
		if (m_logLevel.ordinal() >= LogLevel.WARN.ordinal())
		{
			for (LogDestination dest : m_logDestinations)
				dest.log(LogLevel.WARN, p_header, p_msg, null);
		}
	}

	@Override
	public void warn(String p_header, String p_msg, Exception p_exception) {
		if (m_logLevel.ordinal() >= LogLevel.WARN.ordinal())
		{
			for (LogDestination dest : m_logDestinations)
				dest.log(LogLevel.WARN, p_header, p_msg, p_exception);
		}
	}

	@Override
	public void info(String p_header, String p_msg) {
		if (m_logLevel.ordinal() >= LogLevel.INFO.ordinal())
		{
			for (LogDestination dest : m_logDestinations)
				dest.log(LogLevel.INFO, p_header, p_msg, null);
		}
	}

	@Override
	public void info(String p_header, String p_msg, Exception p_exception) {
		if (m_logLevel.ordinal() >= LogLevel.INFO.ordinal())
		{
			for (LogDestination dest : m_logDestinations)
				dest.log(LogLevel.INFO, p_header, p_msg, p_exception);
		}
	}

	@Override
	public void debug(String p_header, String p_msg) {
		if (m_logLevel.ordinal() >= LogLevel.DEBUG.ordinal())
		{
			for (LogDestination dest : m_logDestinations)
				dest.log(LogLevel.DEBUG, p_header, p_msg, null);
		}
	}

	@Override
	public void debug(String p_header, String p_msg, Exception p_exception) {
		if (m_logLevel.ordinal() >= LogLevel.DEBUG.ordinal())
		{
			for (LogDestination dest : m_logDestinations)
				dest.log(LogLevel.DEBUG, p_header, p_msg, p_exception);
		}
	}

	@Override
	public void trace(String p_header, String p_msg) {
		if (m_logLevel.ordinal() >= LogLevel.TRACE.ordinal())
		{
			for (LogDestination dest : m_logDestinations)
				dest.log(LogLevel.TRACE, p_header, p_msg, null);
		}
	}

	@Override
	public void trace(String p_header, String p_msg, Exception p_exception) {
		if (m_logLevel.ordinal() >= LogLevel.TRACE.ordinal())
		{
			for (LogDestination dest : m_logDestinations)
				dest.log(LogLevel.TRACE, p_header, p_msg, p_exception);
		}
	}
}
