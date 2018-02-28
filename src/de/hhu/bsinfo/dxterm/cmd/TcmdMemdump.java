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

import de.hhu.bsinfo.dxram.chunk.ChunkDebugService;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdin;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Create a full memory dump of the chunk memory
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdMemdump extends AbstractTerminalCommand {
    public TcmdMemdump() {
        super("memdump");
    }

    @Override
    public String getHelp() {
        return "Create a full memory dump of the chunk memory\n" + "Usage: chunkdump <nid> <fileName>\n" +
                "  nid: Node ID of the remote peer to create the dump of\n" + "  fileName: Name of the file to dump the memory to";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServerStdin p_stdin,
            final TerminalServiceAccessor p_services) {
        short nid = p_cmd.getArgNodeId(0, NodeID.INVALID_ID);
        String fileName = p_cmd.getArgString(1, null);

        if (nid == NodeID.INVALID_ID) {
            p_stdout.printlnErr("No nid specified");
            return;
        }

        if (fileName == null) {
            p_stdout.printlnErr("No file name specified");
            return;
        }

        ChunkDebugService chunkDebug = p_services.getService(ChunkDebugService.class);

        p_stdout.printfln("Dumping memory of 0x%X to file %s...", nid, fileName);
        chunkDebug.dumpChunkMemory(fileName, nid);
        p_stdout.println("(Async) Dumping to memory triggered, depending on the memory size, this might take a few seconds");
    }

    @Override
    public List<String> getArgumentCompletionSuggestions(final int p_argumentPos, final TerminalCommandString p_cmdStr,
            final TerminalServiceAccessor p_services) {
        switch (p_argumentPos) {
            case 0:
                return TcmdUtils.getAllOnlinePeerNodeIDsCompSuggestions(p_services);
            default:
                return Collections.emptyList();
        }
    }
}
