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

import de.hhu.bsinfo.dxram.ms.MasterNodeEntry;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;

/**
 * Get a list of available compute groups
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdCompgrpls extends AbstractTerminalCommand {
    public TcmdCompgrpls() {
        super("compgrpls");
    }

    @Override
    public String getHelp() {
        return "Get a list of available compute groups\n" + "Usage: compgrpls";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServiceAccessor p_services) {
        MasterSlaveComputeService mscomp = p_services.getService(MasterSlaveComputeService.class);
        ArrayList<MasterNodeEntry> masters = mscomp.getMasters();

        p_stdout.printfln("List of available compute groups with master nodes (%d):", masters.size());
        for (MasterNodeEntry master : masters) {
            p_stdout.printfln("%d: 0x%X", master.getComputeGroupId(), master.getNodeId());
        }
    }
}
