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

package de.hhu.bsinfo.dxram.term.cmd;

import de.hhu.bsinfo.dxram.chunk.ChunkDebugService;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalCommandContext;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Create a full memory dump of the chunk memory
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdMemdump extends TerminalCommand {
    public TcmdMemdump() {
        super("memdump");
    }

    @Override
    public String getHelp() {
        return "Create a full memory dump of the chunk memory\n" + "Usage: chunkdump <nid> <fileName>\n" +
            "  nid: Node ID of the remote peer to create the dump of\n" + "  fileName: Name of the file to dump the memory to";
    }

    @Override
    public void exec(final String[] p_args, final TerminalCommandContext p_ctx) {
        short nid = p_ctx.getArgNodeId(p_args, 0, NodeID.INVALID_ID);
        String fileName = p_ctx.getArgString(p_args, 1, null);

        if (nid == NodeID.INVALID_ID) {
            p_ctx.printlnErr("No nid specified");
            return;
        }

        if (fileName == null) {
            p_ctx.printlnErr("No file name specified");
            return;
        }

        ChunkDebugService chunkDebug = p_ctx.getService(ChunkDebugService.class);

        p_ctx.printfln("Dumping memory of 0x%X to file %s...", nid, fileName);
        chunkDebug.dumpChunkMemory(fileName, nid);
        p_ctx.println("(Async) Dumping to memory triggered, depending on the memory size, this might take a few seconds");
    }
}
