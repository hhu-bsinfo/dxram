
package de.hhu.bsinfo.dxcompute.ms.tasks;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.term.TerminalDelegate;

public class PrintMemoryStatusToFileTask extends PrintMemoryStatusTaskPayload {

	private String m_path = null;

	public PrintMemoryStatusToFileTask() {
		super(MasterSlaveTaskPayloads.TYPE, MasterSlaveTaskPayloads.SUBTYPE_PRINT_MEMORY_STATUS_FILE_TASK);
	}

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
			file.delete();
			try {
				file.createNewFile();
			} catch (final IOException e) {
				loggerService.error(getClass(), "Creating output file " + m_path + " for memory status failed", e);
				return -2;
			}
		}

		PrintStream out;
		try {
			out = new PrintStream(file);
		} catch (final FileNotFoundException e) {
			loggerService.error(getClass(), "Creating print stream for memory status failed", e);
			return -3;
		}
		printMemoryStatusToOutput(out, chunkService.getStatus());

		out.close();

		return 0;
	}

	@Override
	public boolean terminalCommandCallbackForParameters(final TerminalDelegate p_delegate) {
		m_path = p_delegate.promptForUserInput("outputPath");

		return true;
	}
}
