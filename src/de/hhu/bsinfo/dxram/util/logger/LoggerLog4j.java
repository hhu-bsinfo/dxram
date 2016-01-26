package de.hhu.bsinfo.dxram.util.logger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Log4j implementation for the DXRAM Logger.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class LoggerLog4j implements de.hhu.bsinfo.dxram.util.logger.Logger {

	@Override
	public void setLogLevel(LogLevel p_logLevel) {
		switch (p_logLevel)
		{
			case DISABLED:
				Logger.getRootLogger().setLevel(Level.OFF); break;
			case ERROR:
				Logger.getRootLogger().setLevel(Level.ERROR); break;
			case WARN:
				Logger.getRootLogger().setLevel(Level.WARN); break;
			case INFO:
				Logger.getRootLogger().setLevel(Level.INFO); break;
			case DEBUG:
				Logger.getRootLogger().setLevel(Level.DEBUG); break;
			case TRACE:
				Logger.getRootLogger().setLevel(Level.TRACE); break;
			default:
				assert 1 == 2; break;
		}
	}
	
	@Override
	public void error(String header, String msg) {
		Logger.getLogger(header).error(msg);
	}

	@Override
	public void error(String header, String msg, Exception e) {
		Logger.getLogger(header).error(msg, e);
	}

	@Override
	public void warn(String header, String msg) {
		Logger.getLogger(header).warn(msg);
	}

	@Override
	public void warn(String header, String msg, Exception e) {
		Logger.getLogger(header).warn(msg, e);
	}

	@Override
	public void info(String header, String msg) {
		Logger.getLogger(header).info(msg);
	}

	@Override
	public void info(String header, String msg, Exception e) {
		Logger.getLogger(header).info(msg, e);
	}

	@Override
	public void debug(String header, String msg) {
		Logger.getLogger(header).debug(msg);
	}

	@Override
	public void debug(String header, String msg, Exception e) {
		Logger.getLogger(header).debug(msg, e);
	}

	@Override
	public void trace(String header, String msg) {
		Logger.getLogger(header).trace(msg);
	}

	@Override
	public void trace(String header, String msg, Exception e) {
		Logger.getLogger(header).trace(msg, e);
	}
}
