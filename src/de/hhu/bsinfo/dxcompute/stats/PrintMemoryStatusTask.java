package de.hhu.bsinfo.dxcompute.stats;

import java.io.PrintStream;

import de.hhu.bsinfo.dxcompute.Task;
import de.hhu.bsinfo.dxram.chunk.ChunkService.Status;

public abstract class PrintMemoryStatusTask extends Task {

	public PrintMemoryStatusTask()
	{
		
	}
	
	protected void printMemoryStatusToOutput(final PrintStream p_outputStream)
	{		
		Status status = m_chunkService.getStatus();
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