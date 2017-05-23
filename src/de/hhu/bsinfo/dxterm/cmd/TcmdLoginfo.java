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

import de.hhu.bsinfo.dxram.log.LogService;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;
import de.hhu.bsinfo.utils.NodeID;

/**
 * Prints the log utilization of given peer
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdLoginfo extends AbstractTerminalCommand {
    public TcmdLoginfo() {
        super("loginfo");
    }

    @Override
    public String getHelp() {
        return "Prints the log utilization of given peer\n" + "Usage: loginfo <nid>\n" + "  nid: Node id of the peer";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServiceAccessor p_services) {
        short nid = p_cmd.getArgNodeId(0, NodeID.INVALID_ID);

        if (nid == NodeID.INVALID_ID) {
            p_stdout.printlnErr("None or invalid nid specified");
            return;
        }

        LogService log = p_services.getService(LogService.class);

        p_stdout.println(log.getCurrentUtilization(nid));
    }
}
