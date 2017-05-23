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

import java.util.List;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.lookup.LookupService;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;
import de.hhu.bsinfo.utils.NodeID;

/**
 * Prints a summary of the specified superpeer's metadata
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdMetadata extends AbstractTerminalCommand {
    public TcmdMetadata() {
        super("metadata");
    }

    @Override
    public String getHelp() {
        return "Prints a summary of the specified superpeer's metadata\n" + "Usage: metadatasummary <nid>\n" +
                "  nid: Node id of the superpeer to print the metadata of\n" + "       \"all\" prints metadata of all superpeers";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServiceAccessor p_services) {
        String nidStr = p_cmd.getArgString(0, null);

        if (nidStr == null) {
            p_stdout.printlnErr("No nid specified");
            return;
        }

        if ("all".equals(nidStr)) {
            LookupService lookup = p_services.getService(LookupService.class);
            BootService boot = p_services.getService(BootService.class);
            List<Short> nodeIds = boot.getOnlineNodeIDs();

            for (Short nodeId : nodeIds) {
                NodeRole curRole = boot.getNodeRole(nodeId);
                if (curRole == NodeRole.SUPERPEER) {
                    String summary = lookup.getMetadataSummary(nodeId);
                    p_stdout.printfln("Metadata summary of 0x%X:\n%s", nodeId, summary);
                }
            }
        } else {
            short nid = p_cmd.getArgNodeId(0, NodeID.INVALID_ID);

            if (nid == NodeID.INVALID_ID) {
                p_stdout.printlnErr("No nid specified");
                return;
            }

            LookupService lookup = p_services.getService(LookupService.class);

            String summary = lookup.getMetadataSummary(nid);
            p_stdout.printfln("Metadata summary of 0x%X:\n%s", nid, summary);
        }
    }
}
