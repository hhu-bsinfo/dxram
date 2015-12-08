package de.uniduesseldorf.dxcompute.logger;

public interface LoggerDelegate 
{
	public void log(final LOG_LEVEL p_logLevel, final String m_header, final String p_msg);
}
