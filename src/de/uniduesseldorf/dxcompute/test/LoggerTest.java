package de.uniduesseldorf.dxcompute.test;

import de.uniduesseldorf.dxcompute.logger.LOG_LEVEL;
import de.uniduesseldorf.dxcompute.logger.LoggerDelegate;

public class LoggerTest implements LoggerDelegate
{

	@Override
	public void log(LOG_LEVEL p_logLevel, String m_header, String p_msg) {
		System.out.println("[" + p_logLevel + "][" + m_header + "] " + p_msg);
		
	}

}
