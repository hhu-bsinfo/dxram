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

import de.hhu.bsinfo.dxram.chunk.ChunkRemoveService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalCommandContext;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Remove an existing chunk
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdChunkremove extends AbstractTerminalCommand {
    public TcmdChunkremove() {
        super("chunkremove");
    }

    @Override
    public String getHelp() {
        return "Remove an existing chunk. Usable with either full chunk id or split into nid and lid\n" + "Usage (1): chunkremove <cid>\n" +
            "Usage (2): chunkremove <nid> <lid>\n" + "  cid: Full chunk id of the chunk to remove\n" +
            "  nid: Node id to remove the chunk with specified local id\n" +
            "  lid: Local id of the chunk to remove. If missing node id, current node is assumed";
    }

    @Override
    public void exec(final String[] p_args, final TerminalCommandContext p_ctx) {
        long cid;

        if (p_args.length > 1) {
            short nid = TerminalCommandContext.getArgNodeId(p_args, 0, NodeID.INVALID_ID);
            long lid = TerminalCommandContext.getArgLocalId(p_args, 1, ChunkID.INVALID_ID);
            cid = ChunkID.getChunkID(nid, lid);
        } else {
            cid = TerminalCommandContext.getArgChunkId(p_args, 0, ChunkID.INVALID_ID);
        }

        if (cid == ChunkID.INVALID_ID) {
            TerminalCommandContext.printlnErr("No or invalid cid specified");
            return;
        }

        // don't allow removal of index chunk
        if (ChunkID.getLocalID(cid) == 0) {
            TerminalCommandContext.printlnErr("Removal of index chunk is not allowed");
            return;
        }

        ChunkRemoveService chunk = p_ctx.getService(ChunkRemoveService.class);

        if (chunk.remove(cid) != 1) {
            TerminalCommandContext.printflnErr("Removing chunk with ID 0x%X failed", cid);
        } else {
            TerminalCommandContext.printfln("Chunk 0x%X removed", cid);
        }
    }
}
