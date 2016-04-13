
package de.hhu.bsinfo.dxcompute.ms.tasks;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;

public class PrintMemoryStatusToConsoleTask extends PrintMemoryStatusTaskPayload {

	public PrintMemoryStatusToConsoleTask() {
		super(MasterSlaveTaskPayloads.TYPE, MasterSlaveTaskPayloads.SUBTYPE_PRINT_MEMORY_STATUS_CONSOLE_TASK);
	}

	@Override
	public int execute(final DXRAM p_dxram) {
		ChunkService chunkService = p_dxram.getService(ChunkService.class);
		printMemoryStatusToOutput(System.out, chunkService.getStatus());
		return 0;
	}
}
