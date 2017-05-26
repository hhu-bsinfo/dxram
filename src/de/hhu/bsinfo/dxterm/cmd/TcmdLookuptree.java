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

import de.hhu.bsinfo.dxram.lookup.LookupService;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.LookupTree;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdin;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;
import de.hhu.bsinfo.utils.NodeID;

/**
 * Prints the look up tree of a specified node
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdLookuptree extends AbstractTerminalCommand {
    public TcmdLookuptree() {
        super("lookuptree");
    }

    @Override
    public String getHelp() {
        return "Prints the look up tree of a specified node\n" + "Usage: lookuptree <nid>\n" + "  nid: Node id of the peer to print the lookup tree of";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServerStdin p_stdin,
            final TerminalServiceAccessor p_services) {
        short nid = p_cmd.getArgNodeId(0, NodeID.INVALID_ID);

        if (nid == NodeID.INVALID_ID) {
            p_stdout.printlnErr("No nid specified");
            return;
        }

        LookupService lookup = p_services.getService(LookupService.class);

        short respSuperpeer = lookup.getResponsibleSuperpeer(nid);

        if (respSuperpeer == NodeID.INVALID_ID) {
            p_stdout.printflnErr("No responsible superpeer for 0x%X found", nid);
            return;
        }

        LookupTree tree = lookup.getLookupTreeFromSuperpeer(respSuperpeer, nid);
        p_stdout.printfln("Lookup tree of 0x%X:\n%s", nid, tree);
    }
}
