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

import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxcompute.ms.TaskContext;
import de.hhu.bsinfo.dxram.chunk.ChunkService;

/**
 * Print the current memory status to the console.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class PrintMemoryStatusToConsoleTask extends PrintMemoryStatusTaskPayload {

    /**
     * Constructor
     */
    public PrintMemoryStatusToConsoleTask() {
        super(MasterSlaveTaskPayloads.TYPE, MasterSlaveTaskPayloads.SUBTYPE_PRINT_MEMORY_STATUS_CONSOLE_TASK);
    }

    @Override
    public int execute(final TaskContext p_ctx) {
        ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        printMemoryStatusToOutput(System.out, chunkService.getStatus());
        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {
        // ignore signals
    }
}
