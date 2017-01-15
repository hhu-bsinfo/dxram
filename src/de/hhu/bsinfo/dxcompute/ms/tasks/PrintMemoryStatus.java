package de.hhu.bsinfo.dxcompute.ms.tasks;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import java.io.PrintStream;

/**
 * Helper class to print memory status.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
class PrintMemoryStatus {

    /**
     * Static class
     */
    private PrintMemoryStatus() {

    }

    /**
     * Print the current memory status.
     *
     * @param p_outputStream
     *     Output stream to print to.
     * @param p_status
     *     Status to print.
     */
    static void printMemoryStatusToOutput(final PrintStream p_outputStream, final ChunkService.Status p_status) {
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
