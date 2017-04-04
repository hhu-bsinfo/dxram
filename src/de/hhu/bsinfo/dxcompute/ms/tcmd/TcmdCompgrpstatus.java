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

package de.hhu.bsinfo.dxcompute.ms.tcmd;

import de.hhu.bsinfo.dxcompute.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalCommandContext;

/**
 * Get a list of available compute groups
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdCompgrpstatus extends TerminalCommand {
    public TcmdCompgrpstatus() {
        super("compgrpstatus");
    }

    @Override
    public String getHelp() {
        return "Get the current status of a compute group\n" + "Usage: compgrpstatus <cgid>\n" + "  cgid: Compute group id to get the status from";
    }

    @Override
    public void exec(final String[] p_args, final TerminalCommandContext p_ctx) {
        short cgid = p_ctx.getArgShort(p_args, 0, (short) -1);

        if (cgid == -1) {
            p_ctx.printlnErr("No cgid specified");
            return;
        }

        MasterSlaveComputeService mscomp = p_ctx.getService(MasterSlaveComputeService.class);
        MasterSlaveComputeService.StatusMaster status = mscomp.getStatusMaster(cgid);

        if (status == null) {
            p_ctx.printflnErr("Getting compute group status of group %d failed", cgid);
            return;
        }

        p_ctx.printfln("Status of group %d:\n%s", cgid, status);
    }
}
