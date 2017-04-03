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

package de.hhu.bsinfo.dxram.term.cmd;

import java.util.ArrayList;

import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalCommandContext;
import de.hhu.bsinfo.utils.Pair;

/**
 * List all registered name mappings of the nameservice
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdNamelist extends TerminalCommand {
    public TcmdNamelist() {
        super("namelist");
    }

    @Override
    public String getHelp() {
        return "List all registered name mappings of the nameservice\n" + "Usage: namelist";
    }

    @Override
    public void exec(final String[] p_args, final TerminalCommandContext p_ctx) {
        NameserviceService nameservice = p_ctx.getService(NameserviceService.class);

        ArrayList<Pair<String, Long>> entries = nameservice.getAllEntries();

        p_ctx.printfln("Nameservice entries(%d):", entries.size());

        for (Pair<String, Long> entry : entries) {
            p_ctx.printfln("%s: 0x%X", entry.first(), entry.second());
        }
    }
}
