/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.chunk.tcmd;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalCommandContext;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Get the status of the chunk service/memory from a remote node
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdChunkstatus extends AbstractTerminalCommand {
    public TcmdChunkstatus() {
        super("chunkstatus");
    }

    @Override
    public String getHelp() {
        return "Get the status of the chunk service/memory from a remote node\n" + "Usage: chunkstatus <nid>\n" +
            "  nid: Node ID of the remote peer to get the status from";
    }

    @Override
    public void exec(final String[] p_args, final TerminalCommandContext p_ctx) {
        short nid = p_ctx.getArgNodeId(p_args, 0, NodeID.INVALID_ID);
        boolean migrated = p_ctx.getArgBoolean(p_args, 1, false);

        if (nid == NodeID.INVALID_ID) {
            p_ctx.printlnErr("No nid specified");
            return;
        }

        ChunkService chunk = p_ctx.getService(ChunkService.class);

        MemoryManagerComponent.Status status = chunk.getStatus(nid);

        if (status == null) {
            p_ctx.printlnErr("Getting status failed");
            return;
        }

        p_ctx.println(status.toString());
    }
}
