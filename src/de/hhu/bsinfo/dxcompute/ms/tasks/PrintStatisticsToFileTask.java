
package de.hhu.bsinfo.dxcompute.ms.tasks;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.stats.StatisticsService;
import de.hhu.bsinfo.dxram.term.TerminalDelegate;

/**
 * Print the statistics to a file.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class PrintStatisticsToFileTask extends AbstractPrintStatisticsTask {

	private String m_path;

	/**
	 * Constructor
	 */
	public PrintStatisticsToFileTask() {
		super(MasterSlaveTaskPayloads.TYPE, MasterSlaveTaskPayloads.SUBTYPE_PRINT_STATISTICS_FILE_TASK);
	}

	/**
	 * Set the filepath to the output file to print to.
	 * @param p_path
	 *            Filepath of the file to print to.
	 */
	public void setOutputFilePath(final String p_path) {
		m_path = p_path;
	}

	@Override
	public int execute(final DXRAMServiceAccessor p_dxram) {
		BootService bootService = p_dxram.getService(BootService.class);
		StatisticsService statisticsService = p_dxram.getService(StatisticsService.class);
		LoggerService loggerService = p_dxram.getService(LoggerService.class);

		if (m_path == null) {
			return -1;
		}

		File file = new File(m_path);
		if (file.exists()) {
			file.delete();
			try {
				file.createNewFile();
			} catch (final IOException e) {
				loggerService.error(getClass(), "Creating output file " + m_path + " for statistics failed", e);
				return -2;
			}
		}

		PrintStream out;
		try {
			out = new PrintStream(file);
		} catch (final FileNotFoundException e) {
			loggerService.error(getClass(), "Creating print stream for statistics failed", e);
			return 03;
		}
		printStatisticsToOutput(out, bootService, statisticsService);

		out.close();

		return 0;
	}

	@Override
	public boolean terminalCommandCallbackForParameters(final TerminalDelegate p_delegate) {
		m_path = p_delegate.promptForUserInput("outputPath");

		return true;
	}
}
