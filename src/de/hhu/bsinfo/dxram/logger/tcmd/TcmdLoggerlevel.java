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

package de.hhu.bsinfo.dxram.logger.tcmd;

import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalCommandContext;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Change the output level of the logger
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdLoggerlevel extends TerminalCommand {
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
    public void exec(final String[] p_args, final TerminalCommandContext p_ctx) {
        String level = p_ctx.getArgString(p_args, 0, null);
        short nid = p_ctx.getArgNodeId(p_args, 1, NodeID.INVALID_ID);

        if (level == null) {
            p_ctx.printlnErr("No level specified");
            return;
        }

        LoggerService logger = p_ctx.getService(LoggerService.class);

        if (nid == NodeID.INVALID_ID) {
            LoggerService.setLogLevel(level);
        } else {
            logger.setLogLevel(level, nid);
        }
    }
}
