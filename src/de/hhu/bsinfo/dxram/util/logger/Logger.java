package de.hhu.bsinfo.dxram.util.logger;

import de.hhu.bsinfo.utils.log.LoggerInterface;

/**
 * Extending the generic LoggerInterface for DXRAM.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public interface Logger extends LoggerInterface {
	/**
	 * Set the log level for the logger.
	 * @param p_logLevel New log level to be set.
	 */
	public void setLogLevel(final LogLevel p_logLevel);
}
