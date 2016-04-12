
package de.hhu.bsinfo.dxcompute.ms.tasks;

import java.io.PrintStream;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxram.chunk.ChunkService.Status;

public abstract class PrintMemoryStatusTaskPayload extends AbstractTaskPayload {

	public PrintMemoryStatusTaskPayload(short p_typeId, short p_subtypeId) {
		super(p_typeId, p_subtypeId);
	}

	protected void printMemoryStatusToOutput(final PrintStream p_outputStream, final Status status) {
		p_outputStream.println("---------------------------------------------------------");
		p_outputStream.println("---------------------------------------------------------");
		p_outputStream.println("---------------------------------------------------------");
		p_outputStream.println("Chunk service memory:");
		p_outputStream.println("Total: " + status.getTotalMemory());
		p_outputStream.println("Used: " + (status.getTotalMemory() - status.getFreeMemory()));
		p_outputStream.println("Free: " + status.getFreeMemory());
		p_outputStream.println("---------------------------------------------------------");
		p_outputStream.println("---------------------------------------------------------");
		p_outputStream.println("---------------------------------------------------------");
	}
}