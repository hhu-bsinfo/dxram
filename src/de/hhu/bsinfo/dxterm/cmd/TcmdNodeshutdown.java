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

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Shutdown a DXRAM node
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdNodeshutdown extends AbstractTerminalCommand {
    public TcmdNodeshutdown() {
        super("nodeshutdown");
    }

    @Override
    public String getHelp() {
        return "Shutdown a DXRAM node\n" + "Usage: nodeshutdown <nid> [kill]\n" + "  nid: Id of the node to shutdown\n" +
                "  kill: If specified, true for a hard shutdown (kill process), false for proper soft shutdown (default)";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServiceAccessor p_services) {
        short nid = p_cmd.getArgNodeId(0, NodeID.INVALID_ID);
        boolean kill = p_cmd.getArgBoolean(1, false);

        if (nid == NodeID.INVALID_ID) {
            p_stdout.printlnErr("No node id specified");
            return;
        }

        BootService boot = p_services.getService(BootService.class);

        if (!boot.shutdownNode(nid, kill)) {
            p_stdout.printfln("Shutting down node 0x%X failed", nid);
        } else {
            p_stdout.printfln("Shutting down node 0x%X...", nid);
            p_stdout.printfln("Ignore ERROR messages and continue by pressing <Enter>");
        }
    }
}
