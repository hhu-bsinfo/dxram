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
