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

import java.util.ArrayList;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.lock.AbstractLockService;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalCommandContext;
import de.hhu.bsinfo.ethnet.NodeID;
import de.hhu.bsinfo.utils.Pair;

/**
 * Get the list of all locked chunks of a node
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdChunklocklist extends TerminalCommand {
    public TcmdChunklocklist() {
        super("chunklocklist");
    }

    @Override
    public String getHelp() {
        return "Get the list of all locked chunks of a node\n" + "Usage: chunklocklist <nid>\n" + "  nid: Get the list of locked chunks from a remote node";
    }

    @Override
    public void exec(final String[] p_args, final TerminalCommandContext p_ctx) {
        short nid = p_ctx.getArgNodeId(p_args, 0, NodeID.INVALID_ID);

        if (nid == NodeID.INVALID_ID) {
            p_ctx.printlnErr("No nid specified");
            return;
        }

        BootService boot = p_ctx.getService(BootService.class);
        AbstractLockService lock = p_ctx.getService(AbstractLockService.class);

        ArrayList<Pair<Long, Short>> list = lock.getLockedList(nid);

        if (list == null) {
            p_ctx.printlnErr("Getting list of locked chunks failed");
            return;
        }

        p_ctx.printfln("Locked chunks of 0x%X (%d):", nid, list.size());
        p_ctx.println("<lid: nid that locked the chunk>");
        for (Pair<Long, Short> entry : list) {
            p_ctx.printfln("0x%X: 0x%X", entry.first(), entry.second());
        }
    }
}
