
package de.hhu.bsinfo.utils.log;

/**
 * Interface for a destination to write log data to (console, file,...)
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public interface LogDestination {

	/**
	 * Called when the logger gets a message.
	 * @param p_level
	 *            Level of that message.
	 * @param p_message
	 *            Actual message to be logged
	 */
	void log(final LogLevel p_level, final String p_message);
}
