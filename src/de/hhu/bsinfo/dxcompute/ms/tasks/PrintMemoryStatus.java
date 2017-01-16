package de.hhu.bsinfo.dxcompute.ms.tasks;

import java.io.PrintStream;

import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;

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
    static void printMemoryStatusToOutput(final PrintStream p_outputStream, final MemoryManagerComponent.Status p_status) {
        p_outputStream.println("---------------------------------------------------------");
        p_outputStream.println("---------------------------------------------------------");
        p_outputStream.println("---------------------------------------------------------");
        p_outputStream.println("Key value store memory:");
        p_outputStream.println(p_status);
        p_outputStream.println("---------------------------------------------------------");
        p_outputStream.println("---------------------------------------------------------");
        p_outputStream.println("---------------------------------------------------------");
    }
}
