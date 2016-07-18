
package de.hhu.bsinfo.dxcompute.ms.tasks;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.stats.StatisticsService;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * Print the statistics to a file.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class PrintStatisticsToFileTask extends AbstractPrintStatisticsTask {

	private static final Argument MS_ARG_OUTPUT_PATH =
			new Argument("outputPath", null, false, "Filepath to write the statistics to.");

	private String m_path;

	/**
	 * Constructor
	 */
	public PrintStatisticsToFileTask() {
		super(MasterSlaveTaskPayloads.TYPE, MasterSlaveTaskPayloads.SUBTYPE_PRINT_STATISTICS_FILE_TASK);
	}

	/**
	 * Set the filepath to the output file to print to.
	 *
	 * @param p_path Filepath of the file to print to.
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
			if (!file.delete()) {
				// #if LOGGER >= ERROR
				loggerService.error(getClass(), "Deleting file " + file + " failed.");
				// #endif /* LOGGER >= ERROR */
				return -2;
			}
			try {
				if (!file.createNewFile()) {
					// #if LOGGER >= ERROR
					loggerService.error(getClass(), "Creating output file " + m_path + " for statistics failed");
					// #endif /* LOGGER >= ERROR */
					return -3;
				}
			} catch (final IOException e) {
				// #if LOGGER >= ERROR
				loggerService.error(getClass(), "Creating output file " + m_path + " for statistics failed", e);
				// #endif /* LOGGER >= ERROR */
				return -4;
			}
		}

		PrintStream out;
		try {
			out = new PrintStream(file);
		} catch (final FileNotFoundException e) {
			// #if LOGGER >= ERROR
			loggerService.error(getClass(), "Creating print stream for statistics failed", e);
			// #endif /* LOGGER >= ERROR */
			return -5;
		}
		printStatisticsToOutput(out, bootService, statisticsService);

		out.close();

		return 0;
	}

	@Override
	public void handleSignal(final Signal p_signal) {
		// ignore signals
	}

	@Override
	public void terminalCommandRegisterArguments(final ArgumentList p_argumentList) {
		p_argumentList.setArgument(MS_ARG_OUTPUT_PATH);
	}

	@Override
	public void terminalCommandCallbackForArguments(final ArgumentList p_argumentList) {
		m_path = p_argumentList.getArgumentValue(MS_ARG_OUTPUT_PATH, String.class);
	}
}
