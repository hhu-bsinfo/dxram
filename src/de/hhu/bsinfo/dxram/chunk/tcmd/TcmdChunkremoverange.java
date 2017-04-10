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
import de.hhu.bsinfo.dxram.data.ChunkState;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.data.DummyDataStructure;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalCommandContext;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Remove a range of chunks
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdChunkremoverange extends AbstractTerminalCommand {
    public TcmdChunkremoverange() {
        super("chunkremoverange");
    }

    @Override
    public String getHelp() {
        return "Remove a range of existing chunks.\n" + "Usage (1): chunkremoverange <nid> <lid start> <lid end>\n" +
            "  nid: Node id of the range to remove\n" + "  lid start: Start lid of range (including)\n" + "  lid end: End lid of range (including)\n";
    }

    @Override
    public void exec(final String[] p_args, final TerminalCommandContext p_ctx) {
        short nid = TerminalCommandContext.getArgNodeId(p_args, 0, NodeID.INVALID_ID);
        long lidStart = TerminalCommandContext.getArgChunkId(p_args, 1, ChunkID.INVALID_ID);
        long lidEnd = TerminalCommandContext.getArgChunkId(p_args, 2, ChunkID.INVALID_ID);

        if (nid == NodeID.INVALID_ID) {
            TerminalCommandContext.printlnErr("None or invalid nid specified");
            return;
        }

        if (lidStart == NodeID.INVALID_ID) {
            TerminalCommandContext.printlnErr("None or invalid lid start specified");
            return;
        }

        if (lidEnd == NodeID.INVALID_ID) {
            TerminalCommandContext.printlnErr("None or invalid lid end specified");
            return;
        }

        if (lidEnd < lidStart) {
            TerminalCommandContext.printlnErr("Lid end < start");
            return;
        }

        // don't allow removal of index chunk
        if (lidStart == 0 || lidEnd == 0) {
            TerminalCommandContext.printlnErr("Removal of index chunk is not allowed");
            return;
        }

        ChunkRemoveService chunk = p_ctx.getService(ChunkRemoveService.class);

        DataStructure[] chunks = new DataStructure[(int) (lidEnd - lidStart + 1)];

        int index = 0;
        for (long l = lidStart; l <= lidEnd; l++) {
            chunks[index++] = new DummyDataStructure(ChunkID.getChunkID(nid, l));
        }

        if (chunk.remove(chunks) != chunks.length) {
            for (int i = 0; i < chunks.length; i++) {
                if (chunks[i].getState() != ChunkState.OK) {
                    TerminalCommandContext.printflnErr("Removing chunk 0x%X failed: %s", chunks[i].getID(), chunks[i].getState());
                }
            }
        } else {
            TerminalCommandContext.printfln("Chunk(s) removed");
        }
    }
}
