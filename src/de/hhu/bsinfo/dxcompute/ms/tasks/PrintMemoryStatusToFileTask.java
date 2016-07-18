
package de.hhu.bsinfo.dxcompute.ms.tasks;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * Print the current memory status to a file.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class PrintMemoryStatusToFileTask extends AbstractPrintMemoryStatusTaskPayload {

	private static final Argument MS_ARG_OUTPUT_PATH =
			new Argument("outputPath", null, false, "Filepath to write the memory statys to.");

	private String m_path;

	/**
	 * Constructor
	 */
	public PrintMemoryStatusToFileTask() {
		super(MasterSlaveTaskPayloads.TYPE, MasterSlaveTaskPayloads.SUBTYPE_PRINT_MEMORY_STATUS_FILE_TASK);
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
		ChunkService chunkService = p_dxram.getService(ChunkService.class);
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
					loggerService.error(getClass(), "Creating output file " + m_path + " for memory status failed");
					// #endif /* LOGGER >= ERROR */
					return -3;
				}
			} catch (final IOException e) {
				// #if LOGGER >= ERROR
				loggerService.error(getClass(), "Creating output file " + m_path + " for memory status failed", e);
				// #endif /* LOGGER >= ERROR */
				return -4;
			}
		}

		PrintStream out;
		try {
			out = new PrintStream(file);
		} catch (final FileNotFoundException e) {
			// #if LOGGER >= ERROR
			loggerService.error(getClass(), "Creating print stream for memory status failed", e);
			// #endif /* LOGGER >= ERROR */
			return -5;
		}
		printMemoryStatusToOutput(out, chunkService.getStatus());

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
