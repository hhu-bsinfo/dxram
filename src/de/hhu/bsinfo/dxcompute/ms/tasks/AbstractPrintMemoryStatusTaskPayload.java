
package de.hhu.bsinfo.dxcompute.ms.tasks;

import java.io.PrintStream;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxram.chunk.ChunkService.Status;

/**
 * Base class for printing the current memory status.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
abstract class AbstractPrintMemoryStatusTaskPayload extends AbstractTaskPayload {

	/**
	 * Constructor
	 * Expecting a default constructor for the sub classes extending this
	 * base class, otherwise the createInstance call won't work.
	 * Make sure to register each task payload implementation prior usage.
	 * @param p_typeId
	 *            Type id
	 * @param p_subtypeId
	 *            Subtype id
	 */
	AbstractPrintMemoryStatusTaskPayload(final short p_typeId, final short p_subtypeId) {
		super(p_typeId, p_subtypeId);
	}

	/**
	 * Print the current memory status.
	 * @param p_outputStream
	 *            Output stream to print to.
	 * @param p_status
	 *            Status to print.
	 */
	void printMemoryStatusToOutput(final PrintStream p_outputStream, final Status p_status) {
		p_outputStream.println("---------------------------------------------------------");
		p_outputStream.println("---------------------------------------------------------");
		p_outputStream.println("---------------------------------------------------------");
		p_outputStream.println("Chunk service memory:");
		p_outputStream.println("Total: " + p_status.getTotalMemory());
		p_outputStream.println("Used: " + (p_status.getTotalMemory() - p_status.getFreeMemory()));
		p_outputStream.println("Free: " + p_status.getFreeMemory());
		p_outputStream.println("---------------------------------------------------------");
		p_outputStream.println("---------------------------------------------------------");
		p_outputStream.println("---------------------------------------------------------");
	}
}
