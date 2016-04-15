
package de.hhu.bsinfo.utils.log;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Implementation to log to a file.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class LogDestinationFile implements LogDestination {

	private File m_logFile;
	private FileWriter m_logWriter;

	/**
	 * Constructor
	 * @param p_pathLogFile
	 *            Path + filename for the log file.
	 * @param p_backupOld
	 *            True to backup the previous log file with the same name, false to overwrite.
	 */
	public LogDestinationFile(final String p_pathLogFile, final boolean p_backupOld) {
		m_logFile = new File(p_pathLogFile);
		String pathWithoutExt = p_pathLogFile.substring(0, p_pathLogFile.lastIndexOf("."));
		String ext = p_pathLogFile.substring(p_pathLogFile.lastIndexOf(".") + 1);

		// backup old log file
		if (m_logFile.exists() && p_backupOld) {
			// test for .i names until a name is found that does not exist
			int i = 0;
			File f;
			while (true) {
				f = new File(pathWithoutExt + "." + i + "." + ext);
				if (!f.exists()) {
					break;
				}
				i++;
			}
			m_logFile.renameTo(f);
		}

		// create log file if it does not exist
		if (!m_logFile.exists()) {
			try {
				m_logFile.createNewFile();
			} catch (final IOException e) {
				System.out.println("ERROR: Could not create logfile! Reason: " + e.getMessage());
			}
		}

		// initialize writer
		try {
			m_logWriter = new FileWriter(m_logFile);
		} catch (final IOException e) {
			System.out.println("ERROR: Could not get Writer for logfile! Reason: " + e.getMessage());
		}
	}

	@Override
	public void log(final LogLevel p_level, final String p_message) {
		try {
			m_logWriter.append(p_message + "\n");
			m_logWriter.flush();
		} catch (final IOException e) {
			System.out.println("ERROR: Unable to write to logfile! Reason: " + e.getMessage());
		}
	}
}
