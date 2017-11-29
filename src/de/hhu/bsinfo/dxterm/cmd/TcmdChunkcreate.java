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

package de.hhu.bsinfo.dxterm.cmd;

import java.util.Collections;
import java.util.List;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdin;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Create a chunk on a remote node
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdChunkcreate extends AbstractTerminalCommand {
    public TcmdChunkcreate() {
        super("chunkcreate");
    }

    @Override
    public String getHelp() {
        return "Create a chunk on a remote node\n" + "Usage: chunkcreate <size> <nid>\n" + "  size: Size of the chunk to create\n" +
                "  nid: Node id of the peer to create the chunk on";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServerStdin p_stdin,
            final TerminalServiceAccessor p_services) {
        int size = p_cmd.getArgInt(0, -1);
        short nid = p_cmd.getArgNodeId(1, NodeID.INVALID_ID);

        if (size == -1) {
            p_stdout.printlnErr("No size specified");
            return;
        }

        if (nid == NodeID.INVALID_ID) {
            p_stdout.printlnErr("No nid specified");
            return;
        }

        BootService boot = p_services.getService(BootService.class);
        ChunkService chunk = p_services.getService(ChunkService.class);

        long[] chunkIDs;

        if (boot.getNodeID() == nid) {
            chunkIDs = chunk.create(size, 1);
        } else {
            chunkIDs = chunk.createRemote(nid, size);
        }

        p_stdout.printfln("Created chunk of size %d: 0x%X", size, chunkIDs[0]);
    }

    @Override
    public List<String> getArgumentCompletionSuggestions(final int p_argumentPos, final TerminalCommandString p_cmdStr,
            final TerminalServiceAccessor p_services) {
        if (p_argumentPos == 1) {
            return TcmdUtils.getAllOnlinePeerNodeIDsCompSuggestions(p_services);
        }

        return Collections.emptyList();
    }
}
