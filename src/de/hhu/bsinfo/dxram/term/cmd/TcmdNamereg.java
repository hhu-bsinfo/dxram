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

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalCommandContext;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Get the chunk id for a registered name mapping
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdNamereg extends TerminalCommand {
    public TcmdNamereg() {
        super("namereg");
    }

    @Override
    public String getHelp() {
        return "Get the chunk id for a registered name mapping\n" + "Usage (1): namereg <cid> <name> \n" + "Usage (2): namereg <nid> <lid> <name>\n" +
            "  cid: Full chunk ID of the chunk to register as string\n" + "  nid: Separate local id part of the chunk to register\n" +
            "  lid: Separate node id part of the chunk to register\n" + "  name: Name to register the chunk id for";
    }

    @Override
    public void exec(final String[] p_args, final TerminalCommandContext p_ctx) {
        long cid;
        String name;

        if (p_args.length > 2) {
            short nid = p_ctx.getArgNodeId(p_args, 0, NodeID.INVALID_ID);
            long lid = p_ctx.getArgLocalId(p_args, 1, ChunkID.INVALID_ID);
            cid = ChunkID.getChunkID(nid, lid);
            name = p_ctx.getArgString(p_args, 2, null);
        } else {
            cid = p_ctx.getArgChunkId(p_args, 0, ChunkID.INVALID_ID);
            name = p_ctx.getArgString(p_args, 1, null);
        }

        if (name == null) {
            p_ctx.printlnErr("No name specified");
            return;
        }

        if (cid == ChunkID.INVALID_ID) {
            p_ctx.printlnErr("No chunk id or invalid id specified");
            return;
        }

        NameserviceService nameservice = p_ctx.getService(NameserviceService.class);
        nameservice.register(cid, name);
    }
}
