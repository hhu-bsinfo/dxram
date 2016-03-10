package de.hhu.bsinfo.utils.log;

public interface LogDestination {
	/** Gets called when the log level changes.
	 * 
	 *  Store the assigned level to implement
	 *  log message filtering.
	 * 
	 * @param p_level Level assigned to this destination.
	 */
	public void setLogLevel(final LogLevel p_level);
	
	/** Gets called when the destination is added to the logger
	 *  and tells us to prepare for the logging process
	 */
	public void logStart();
	
	/** Called when the logger gets a message.
	 * 
	 * @param p_level Level of that message.
	 * @param p_header Message header.
	 * @param p_message Actual message to be logged
	 * @param p_exception Optional exception to be logged (might be null).
	 */
	public void log(final LogLevel p_level, final String p_header, final String p_message, final Exception p_exception);
	
	/** Called when logger gets destroyed.
	 *  Trigger any cleanup that's necessary for your destination.
	 */
	public void logEnd();
}
