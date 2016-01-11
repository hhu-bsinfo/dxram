package de.uniduesseldorf.utils.log;

public class LoggerNull implements LoggerInterface {

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
