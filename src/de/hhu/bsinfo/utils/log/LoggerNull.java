package de.hhu.bsinfo.utils.log;

/**
 * Null logger/dummy stubbing all log calls. To be used if you don't want to use any logging.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class LoggerNull implements LoggerInterface {

	@Override
	public void setLogLevel(LogLevel p_logLevel) {
	}
	
	@Override
	public void error(String p_header, String p_msg) {
	}

	@Override
	public void error(String p_header, String p_msg, Exception p_e) {
	}

	@Override
	public void warn(String p_header, String p_msg) {
	}

	@Override
	public void warn(String p_header, String p_msg, Exception p_e) {
	}

	@Override
	public void info(String p_header, String p_msg) {
	}

	@Override
	public void info(String p_header, String p_msg, Exception p_e) {
	}

	@Override
	public void debug(String p_header, String p_msg) {
	}

	@Override
	public void debug(String p_header, String p_msg, Exception p_e) {
	}

	@Override
	public void trace(String p_header, String p_msg) {
	}

	@Override
	public void trace(String p_header, String p_msg, Exception p_e) {
	}
}
