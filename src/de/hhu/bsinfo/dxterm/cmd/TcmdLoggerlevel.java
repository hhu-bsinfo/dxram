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

import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdin;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;
import de.hhu.bsinfo.utils.NodeID;

/**
 * Change the output level of the logger
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdLoggerlevel extends AbstractTerminalCommand {
    public TcmdLoggerlevel() {
        super("loggerlevel");
    }

    @Override
    public String getHelp() {
        return "Change the output level of the logger\n" + "Usage: loggerlevel <level> [nid]\n" +
                "  level: Log level to set, available levels (str): disabled, error, warn, info, debug, trace\n" +
                "  nid: Change the log level of another node, defaults to current node";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServerStdin p_stdin,
            final TerminalServiceAccessor p_services) {
        String level = p_cmd.getArgString(0, null);
        short nid = p_cmd.getArgNodeId(1, NodeID.INVALID_ID);

        if (level == null) {
            p_stdout.printlnErr("No level specified");
            return;
        }

        LoggerService logger = p_services.getService(LoggerService.class);

        if (nid == NodeID.INVALID_ID) {
            LoggerService.setLogLevel(level);
        } else {
            logger.setLogLevel(level, nid);
        }
    }
}
