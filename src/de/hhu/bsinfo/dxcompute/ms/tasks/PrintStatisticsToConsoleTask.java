
package de.hhu.bsinfo.dxcompute.ms.tasks;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.stats.StatisticsService;

public class PrintStatisticsToConsoleTask extends PrintStatisticsTask {

	public PrintStatisticsToConsoleTask(short p_typeId, short p_subtypeId) {
		super(p_typeId, p_subtypeId);
	}

	@Override
	public int execute(final DXRAM p_dxram) {
		BootService bootService = p_dxram.getService(BootService.class);
		StatisticsService statisticsService = p_dxram.getService(StatisticsService.class);
		printStatisticsToOutput(System.out, bootService, statisticsService);
		return 0;
	}
}
