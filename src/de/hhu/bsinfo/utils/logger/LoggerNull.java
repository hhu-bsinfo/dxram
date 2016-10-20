
package de.hhu.bsinfo.utils.logger;

/**
 * Null logger/dummy stubbing all log calls. To be used if you don't want to use any logging.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class LoggerNull implements LoggerInterface {

	@Override
	public void setLogLevel(final LogLevel p_logLevel) {}

	@Override
	public void error(final String p_header, final String p_msg) {}

	@Override
	public void error(final String p_header, final String p_msg, final Exception p_e) {}

	@Override
	public void warn(final String p_header, final String p_msg) {}

	@Override
	public void warn(final String p_header, final String p_msg, final Exception p_e) {}

	@Override
	public void info(final String p_header, final String p_msg) {}

	@Override
	public void info(final String p_header, final String p_msg, final Exception p_e) {}

	@Override
	public void debug(final String p_header, final String p_msg) {}

	@Override
	public void debug(final String p_header, final String p_msg, final Exception p_e) {}

	@Override
	public void trace(final String p_header, final String p_msg) {}

	@Override
	public void trace(final String p_header, final String p_msg, final Exception p_e) {}
}
