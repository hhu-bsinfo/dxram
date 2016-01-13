package de.hhu.bsinfo.utils.log;

public interface LoggerInterface {
	public void error(final String p_header, final String p_msg);
	
	public void error(final String p_header, final String p_msg, final Exception p_e);
	
	public void warn(final String p_header, final String p_msg);
	
	public void warn(final String p_header, final String p_msg, final Exception p_e);
	
	public void info(final String p_header, final String p_msg);
	
	public void info(final String p_header, final String p_msg, final Exception p_e);
	
	public void debug(final String p_header, final String p_msg);
	
	public void debug(final String p_header, final String p_msg, final Exception p_e);
	
	public void trace(final String p_header, final String p_msg);
	
	public void trace(final String p_header, final String p_msg, final Exception p_e);
}
