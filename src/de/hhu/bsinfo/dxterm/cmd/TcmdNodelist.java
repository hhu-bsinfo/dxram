/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxterm.cmd;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.util.NodeCapabilities;
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

    private static final String ARG_CAPABILITIES = "cap";

    private static final String ARG_NODEROLE = "role";

    public TcmdNodelist() {
        super("nodelist");
    }

    @Override
    public String getHelp() {
        return "List all available nodes or nodes of a specific type\n\n" +
                "Usage: nodelist [role] [cap]\n\n" +
                "\t role \t\t Filter list by role if specified\n" +
                "\t cap \t\t Filter list by capabilities";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServerStdin p_stdin,
            final TerminalServiceAccessor p_services) {

        BootService boot = p_services.getService(BootService.class);

        List<Short> nodeIds = boot.getOnlineNodeIDs();

        final NodeRole nodeRole = p_cmd.getNamedArgument(ARG_NODEROLE, NodeRole::toNodeRole, null);

        final int capability = p_cmd.getNamedArgument(ARG_CAPABILITIES, Integer::valueOf, NodeCapabilities.NONE);

        List<NodeEntry> filteredNodes = nodeIds.stream()
                .map(nodeId -> new NodeEntry(nodeId, boot.getNodeRole(nodeId), boot.getNodeCapabilities(nodeId)))
                .filter(entry -> nodeRole == null || entry.getRole() == nodeRole)
                .filter(entry -> NodeCapabilities.supportsAll(entry.getCapabilities(), capability))
                .collect(Collectors.toList());

        p_stdout.printfln("Total available nodes (%d):", nodeIds.size());

        for (NodeEntry entry : filteredNodes) {

            p_stdout.printfln("\t0x%04X \t %s \t %s", entry.getId(), entry.getRole(), NodeCapabilities.toString(entry.getCapabilities()));
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

    private static class NodeEntry {

        private final short m_id;

        private final NodeRole m_role;

        private final int m_capabilities;

        NodeEntry(short p_id, NodeRole p_role, int p_capabilities) {
            m_id = p_id;

            m_role = p_role;

            m_capabilities = p_capabilities;
        }

        public short getId() {

            return m_id;
        }

        public NodeRole getRole() {

            return m_role;
        }

        public int getCapabilities() {

            return m_capabilities;
        }
    }
}
