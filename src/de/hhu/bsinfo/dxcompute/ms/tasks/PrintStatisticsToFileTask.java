
package de.hhu.bsinfo.dxcompute.ms.tasks;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.stats.StatisticsService;

public class PrintStatisticsToFileTask extends PrintStatisticsTask {

	private String m_path = null;

	public PrintStatisticsToFileTask(short p_typeId, short p_subtypeId) {
		super(p_typeId, p_subtypeId);
	}

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
}
