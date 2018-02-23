/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdin;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Get the chunk id for a registered name mapping
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdNamereg extends AbstractTerminalCommand {
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
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServerStdin p_stdin,
            final TerminalServiceAccessor p_services) {
        long cid;
        String name;

        if (p_cmd.getArgc() > 2) {
            short nid = p_cmd.getArgNodeId(0, NodeID.INVALID_ID);
            long lid = p_cmd.getArgLocalId(1, ChunkID.INVALID_ID);
            cid = ChunkID.getChunkID(nid, lid);
            name = p_cmd.getArgString(2, null);
        } else {
            cid = p_cmd.getArgChunkId(0, ChunkID.INVALID_ID);
            name = p_cmd.getArgString(1, null);
        }

        if (name == null) {
            p_stdout.printlnErr("No name specified");
            return;
        }

        if (cid == ChunkID.INVALID_ID) {
            p_stdout.printlnErr("No chunk id or invalid id specified");
            return;
        }

        NameserviceService nameservice = p_services.getService(NameserviceService.class);
        nameservice.register(cid, name);
    }

    @Override
    public List<String> getArgumentCompletionSuggestions(final int p_argumentPos, final TerminalCommandString p_cmdStr,
            final TerminalServiceAccessor p_services) {
        return Collections.emptyList();
    }
}
