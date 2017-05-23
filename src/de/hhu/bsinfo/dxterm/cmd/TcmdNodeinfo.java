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
import de.hhu.bsinfo.utils.NodeID;

/**
 * Get information about either the current node or another node in the network
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdNodeinfo extends AbstractTerminalCommand {
    public TcmdNodeinfo() {
        super("nodeinfo");
    }

    @Override
    public String getHelp() {
        return "Get information about either the current node or another node in the network\n" + "Usage (1): nodeinfo [nid]\n" +
                "  nid: If specified, gets information of this node";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServiceAccessor p_services) {
        short nid = p_cmd.getArgNodeId(0, NodeID.INVALID_ID);

        BootService boot = p_services.getService(BootService.class);

        if (nid != NodeID.INVALID_ID) {
            if (boot.nodeAvailable(nid)) {
                p_stdout.printfln("Node info 0x%X:", nid);
                p_stdout.printfln("\tRole: %s", boot.getNodeRole(nid));
                p_stdout.printfln("\tAddress: %s", boot.getNodeAddress(nid));
            } else {
                p_stdout.printfln("Not available.");
            }
        } else {
            p_stdout.printfln("Node info 0x%X:", boot.getNodeID());
            p_stdout.printfln("\tRole: %s", boot.getNodeRole());
            p_stdout.printfln("\tAddress: %s", boot.getNodeAddress(boot.getNodeID()));
        }
    }
}
