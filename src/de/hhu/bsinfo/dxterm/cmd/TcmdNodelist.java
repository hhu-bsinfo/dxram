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

import java.util.ArrayList;
import java.util.List;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdin;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;

/**
 * List all available nodes or nodes of a specific type
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdNodelist extends AbstractTerminalCommand {
    public TcmdNodelist() {
        super("nodelist");
    }

    @Override
    public String getHelp() {
        return "List all available nodes or nodes of a specific type\n" + "Usage: nodelist [role]\n" + "  role: Filter list by role if specified";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServerStdin p_stdin,
            final TerminalServiceAccessor p_services) {
        NodeRole nodeRole = p_cmd.getArgNodeRole(0, null);

        BootService boot = p_services.getService(BootService.class);

        List<Short> nodeIds = boot.getOnlineNodeIDs();

        p_stdout.printfln("Total available nodes (%d):", nodeIds.size());

        for (Short nodeId : nodeIds) {
            NodeRole curRole = boot.getNodeRole(nodeId);

            if (nodeRole == null || curRole == nodeRole) {
                p_stdout.printfln("\t0x%04X   %s", nodeId, curRole);
            }
        }
    }

    @Override
    public List<String> getArgumentCompletionSuggestions(final int p_argumentPos, final TerminalCommandString p_cmdStr,
            final TerminalServiceAccessor p_services) {
        List<String> list = new ArrayList<String>();

        switch (p_argumentPos) {
            case 0:
                list.add(NodeRole.SUPERPEER_STR);
                list.add(NodeRole.PEER_STR);

                break;

            default:
                break;
        }

        return list;
    }
}
