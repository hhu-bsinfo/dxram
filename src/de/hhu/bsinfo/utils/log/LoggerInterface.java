package de.hhu.bsinfo.utils.log;

/**
 * Generic interface for any type of logger. Use this to enable different
 * implementations of loggers (libraries, custom created loggers) to be used 
 * in the project/application.
 * 
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public interface LoggerInterface {
	/**
	 * Log an error message.
	 * @param p_header Message header like instance ID, class the message comes from, filename, ...
	 * @param p_msg Actual log message.
	 */
	public void error(final String p_header, final String p_msg);
	
	/**
	 * Log an error message with an exception attached.
	 * @param p_header Message header like instance ID, class the message comes from, filename, ...
	 * @param p_msg Actual log message.
	 * @param p_e Exception attached to this message.
	 */
	public void error(final String p_header, final String p_msg, final Exception p_e);
	
	/**
	 * Log a warning message.
	 * @param p_header Message header like instance ID, class the message comes from, filename, ...
	 * @param p_msg Actual log message.
	 */
	public void warn(final String p_header, final String p_msg);
	
	/**
	 * Log a warning message with an exception attached.
	 * @param p_header Message header like instance ID, class the message comes from, filename, ...
	 * @param p_msg Actual log message.
	 * @param p_e Exception attached to this message.
	 */
	public void warn(final String p_header, final String p_msg, final Exception p_e);
	
	/**
	 * Log an info message.
	 * @param p_header Message header like instance ID, class the message comes from, filename, ...
	 * @param p_msg Actual log message.
	 */
	public void info(final String p_header, final String p_msg);
	
	/**
	 * Log an info message with an exception attached.
	 * @param p_header Message header like instance ID, class the message comes from, filename, ...
	 * @param p_msg Actual log message.
	 * @param p_e Exception attached to this message.
	 */
	public void info(final String p_header, final String p_msg, final Exception p_e);
	
	/**
	 * Log a debug message.
	 * @param p_header Message header like instance ID, class the message comes from, filename, ...
	 * @param p_msg Actual log message.
	 */
	public void debug(final String p_header, final String p_msg);
	
	/**
	 * Log a debug message with an exception attached.
	 * @param p_header Message header like instance ID, class the message comes from, filename, ...
	 * @param p_msg Actual log message.
	 * @param p_e Exception attached to this message.
	 */
	public void debug(final String p_header, final String p_msg, final Exception p_e);
	
	/**
	 * Log a trace message.
	 * @param p_header Message header like instance ID, class the message comes from, filename, ...
	 * @param p_msg Actual log message.
	 */
	public void trace(final String p_header, final String p_msg);
	
	/**
	 * Log a trace message with an exception attached.
	 * @param p_header Message header like instance ID, class the message comes from, filename, ...
	 * @param p_msg Actual log message.
	 * @param p_e Exception attached to this message.
	 */
	public void trace(final String p_header, final String p_msg, final Exception p_e);
}
