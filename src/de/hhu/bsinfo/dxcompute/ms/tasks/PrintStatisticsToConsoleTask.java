
package de.hhu.bsinfo.dxcompute.ms.tasks;

import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxcompute.ms.TaskContext;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.stats.StatisticsService;

/**
 * Print statistics to the console.
 *
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
	public int execute(final TaskContext p_ctx) {
		BootService bootService = p_ctx.getDXRAMServiceAccessor().getService(BootService.class);
		StatisticsService statisticsService = p_ctx.getDXRAMServiceAccessor().getService(StatisticsService.class);
		printStatisticsToOutput(System.out, bootService, statisticsService);
		return 0;
	}

	@Override
	public void handleSignal(final Signal p_signal) {
		// ignore signals
	}
}
