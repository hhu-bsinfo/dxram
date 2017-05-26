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

import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdin;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;

/**
 * Get a list of available compute groups
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdCompgrpstatus extends AbstractTerminalCommand {
    public TcmdCompgrpstatus() {
        super("compgrpstatus");
    }

    @Override
    public String getHelp() {
        return "Get the current status of a compute group\n" + "Usage: compgrpstatus <cgid>\n" + "  cgid: Compute group id to get the status from";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServerStdin p_stdin,
            final TerminalServiceAccessor p_services) {
        short cgid = p_cmd.getArgShort(0, (short) -1);

        if (cgid == -1) {
            p_stdout.printlnErr("No cgid specified");
            return;
        }

        MasterSlaveComputeService mscomp = p_services.getService(MasterSlaveComputeService.class);
        MasterSlaveComputeService.StatusMaster status = mscomp.getStatusMaster(cgid);

        if (status == null) {
            p_stdout.printflnErr("Getting compute group status of group %d failed", cgid);
            return;
        }

        p_stdout.printfln("Status of group %d:\n%s", cgid, status);
    }
}
