
package de.hhu.bsinfo.utils.log;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Implementation to log to a file.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class LogDestinationFile implements LogDestination {

	private LogLevel m_logLevel = LogLevel.DEBUG;
	private long m_timeMsStarted = 0;
	private File m_logFile = null;
	private FileWriter m_logWriter = null;

	public LogDestinationFile(final String p_pathLogFile, boolean p_backupOld)
	{
		m_logFile = new File(p_pathLogFile);
		String pathWithoutExt = p_pathLogFile.substring(0, p_pathLogFile.lastIndexOf("."));
		String ext = p_pathLogFile.substring(p_pathLogFile.lastIndexOf(".") + 1);

		// backup old log file
		if (m_logFile.exists() && p_backupOld)
		{
			// test for .i names until a name is found that does not exist
			int i = 0;
			File f;
			while ((f = new File(pathWithoutExt + "." + i + "." + ext)).exists()) {
				i++;
			}
			m_logFile.renameTo(f);
		}

		// create log file if it does not exist
		if (!m_logFile.exists()) {
			try {
				m_logFile.createNewFile();
			} catch (IOException e) {
				System.out.println("ERROR: Could not create logfile! Reason: " + e.getMessage());
			}
		}

		// initialize writer
		try {
			m_logWriter = new FileWriter(m_logFile);
		} catch (IOException e) {
			System.out.println("ERROR: Could not get Writer for logfile! Reason: " + e.getMessage());
		}
	}

	@Override
	public void setLogLevel(LogLevel p_level) {
		m_logLevel = p_level;
	}

	@Override
	public void logStart() {
		try {
			if (m_logLevel.ordinal() > LogLevel.DISABLED.ordinal())
			{
				m_logWriter.append("--------------------------------------\n");
				m_logWriter.append("Log started: " + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())).toString() + "\n");
				m_logWriter.append("--------------------------------------\n");
				m_logWriter.flush();
			}
		} catch (IOException e) {
			System.out.println("ERROR: Unable to write to logfile! Reason: " + e.getMessage());
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
					str += "#### " + ste + "\n";
				}
			}
			try {
				m_logWriter.append(str + "\n");
				m_logWriter.flush();
			} catch (IOException e) {
				System.out.println("ERROR: Unable to write to logfile! Reason: " + e.getMessage());
			}
		}
	}

	@Override
	public void logEnd() {
		try {
			if (m_logLevel.ordinal() > LogLevel.DISABLED.ordinal())
			{
				m_logWriter.append("--------------------------------------\n");
				m_logWriter.append("Log finished: " + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())).toString() + "\n");
				m_logWriter.append("--------------------------------------\n");
				m_logWriter.flush();
			}
			m_logWriter.close();
		} catch (IOException e) {
			// don't care if that goes wrong
			System.out.println("ERROR: Flushing and closing log writer failed: " + e.getMessage());
		}
	}

}
