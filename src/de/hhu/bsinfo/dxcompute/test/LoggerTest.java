package de.hhu.bsinfo.dxcompute.test;

import de.hhu.bsinfo.dxcompute.logger.LOG_LEVEL;
import de.hhu.bsinfo.dxcompute.logger.LoggerDelegate;

public class LoggerTest implements LoggerDelegate
{

	@Override
	public void log(LOG_LEVEL p_logLevel, String m_header, String p_msg) {
		System.out.println("[" + p_logLevel + "][" + m_header + "] " + p_msg);
		
	}

}
