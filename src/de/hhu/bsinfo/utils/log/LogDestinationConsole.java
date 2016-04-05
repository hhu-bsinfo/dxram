
package de.hhu.bsinfo.utils.log;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Implementation to log to the console.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class LogDestinationConsole implements LogDestination {

	private LogLevel m_logLevel = LogLevel.DEBUG;
	private long m_timeMsStarted = 0;

	@Override
	public void setLogLevel(LogLevel p_level) {
		m_logLevel = p_level;
	}

	@Override
	public void logStart() {
		if (m_logLevel.ordinal() > LogLevel.DISABLED.ordinal())
		{
			System.out.println("--------------------------------------");
			System.out.println("Log started: " + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())).toString());
			System.out.println("--------------------------------------");
		}
		m_timeMsStarted = System.currentTimeMillis();
	}

	@Override
	public void log(LogLevel p_level, String p_header, String p_message, Exception p_exception) {
		if (p_level.ordinal() <= m_logLevel.ordinal()) {
			String str = new String();
			str += "[" + (System.currentTimeMillis() - m_timeMsStarted) + "]";
			str += "[" + p_level.toString() + "]";
			str += "[TID: " + Thread.currentThread().getId() + "]";
			str += "[" + p_header + "] ";
			str += p_message;
			if (p_exception != null) {
				str += "\n##### " + p_exception + "\n";
				for (StackTraceElement ste : p_exception.getStackTrace()) {
					str += "####" + ste + "\n";
				}
			}
			System.out.println(str);
		}
	}

	@Override
	public void logEnd() {
		if (m_logLevel.ordinal() > LogLevel.DISABLED.ordinal())
		{
			System.out.println("--------------------------------------");
			System.out.println("Log finished: " + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())).toString());
			System.out.println("--------------------------------------");
		}
	}

}
