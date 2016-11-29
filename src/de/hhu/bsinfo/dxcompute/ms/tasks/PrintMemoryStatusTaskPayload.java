/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxcompute.ms.tasks;

import java.io.PrintStream;

import de.hhu.bsinfo.dxcompute.ms.TaskPayload;
import de.hhu.bsinfo.dxram.chunk.ChunkService.Status;

/**
 * Base class for printing the current memory status.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
abstract class PrintMemoryStatusTaskPayload extends TaskPayload {

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
    PrintMemoryStatusTaskPayload(final short p_typeId, final short p_subtypeId) {
        super(p_typeId, p_subtypeId, NUM_REQUIRED_SLAVES_ARBITRARY);
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
