
package de.hhu.bsinfo.dxcompute.ms.tasks;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.stats.StatisticsService;

/**
 * Print statistics to the console.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class PrintStatisticsToConsoleTask extends AbstractPrintStatisticsTask {

	/**
	 * Constructor
	 */
	public PrintStatisticsToConsoleTask() {
		super(MasterSlaveTaskPayloads.TYPE, MasterSlaveTaskPayloads.SUBTYPE_PRINT_STATISTICS_CONSOLE_TASK);
	}

	@Override
	public int execute(final DXRAMServiceAccessor p_dxram) {
		BootService bootService = p_dxram.getService(BootService.class);
		StatisticsService statisticsService = p_dxram.getService(StatisticsService.class);
		printStatisticsToOutput(System.out, bootService, statisticsService);
		return 0;
	}
}
