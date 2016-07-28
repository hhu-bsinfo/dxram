
package de.hhu.bsinfo.dxcompute.ms.tasks;

import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;

/**
 * Print the current memory status to the console.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class PrintMemoryStatusToConsoleTask extends AbstractPrintMemoryStatusTaskPayload {

	/**
	 * Constructor
	 */
	public PrintMemoryStatusToConsoleTask() {
		super(MasterSlaveTaskPayloads.TYPE, MasterSlaveTaskPayloads.SUBTYPE_PRINT_MEMORY_STATUS_CONSOLE_TASK);
	}

	@Override
	public int execute(final DXRAMServiceAccessor p_dxram) {
		ChunkService chunkService = p_dxram.getService(ChunkService.class);
		printMemoryStatusToOutput(System.out, chunkService.getStatus());
		return 0;
	}

	@Override
	public void handleSignal(final Signal p_signal) {
		// ignore signals
	}
}
