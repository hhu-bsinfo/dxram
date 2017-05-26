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

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkIDRanges;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdin;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;
import de.hhu.bsinfo.utils.NodeID;

/**
 * Get a list of chunk id ranges from a peer holding chunks
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdChunklist extends AbstractTerminalCommand {
    public TcmdChunklist() {
        super("chunklist");
    }

    @Override
    public String getHelp() {
        return "Get a list of chunk id ranges from a peer holding chunks (migrated chunks optional)\n" + "Usage: chunklist <nid> [migrated]\n" +
                "  nid: Node ID of the remote peer to get the list from\n" + "  migrated: List the migrated chunks as well (default: false)";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServerStdin p_stdin,
            final TerminalServiceAccessor p_services) {
        short nid = p_cmd.getArgNodeId(0, NodeID.INVALID_ID);
        boolean migrated = p_cmd.getArgBoolean(1, false);

        if (nid == NodeID.INVALID_ID) {
            p_stdout.printlnErr("No nid specified");
            return;
        }

        ChunkService chunk = p_services.getService(ChunkService.class);

        ChunkIDRanges chunkRanges = chunk.getAllLocalChunkIDRanges(nid);

        if (chunkRanges == null) {
            p_stdout.printlnErr("Getting chunk ranges failed");
            return;
        }

        p_stdout.printfln("Locally created chunk id ranges of 0x%X:\n%s", nid, chunkRanges);

        if (migrated) {
            chunkRanges = chunk.getAllMigratedChunkIDRanges(nid);

            if (chunkRanges == null) {
                p_stdout.printlnErr("Getting migrated chunk ranges failed");
                return;
            }

            p_stdout.printfln("Migrated chunk id ranges of 0x%X (%d):\n%s", nid, chunkRanges);
        }
    }

    @Override
    public List<String> getArgumentCompletionSuggestions(final int p_argumentPos, final TerminalCommandString p_cmdStr,
            final TerminalServiceAccessor p_services) {
        switch (p_argumentPos) {
            case 0:
                return TcmdUtils.getAllOnlinePeerNodeIDsCompSuggestions(p_services);
            case 1:
                return TcmdUtils.getBooleanCompSuggestions();
            default:
                return Collections.emptyList();
        }
    }
}
