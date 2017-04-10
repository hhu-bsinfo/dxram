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

package de.hhu.bsinfo.dxram.nameservice.tcmd;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalCommandContext;

/**
 * Get a nameservice entry
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdNameget extends AbstractTerminalCommand {
    public TcmdNameget() {
        super("nameget");
    }

    @Override
    public String getHelp() {
        return "Get a nameservice entry\n" + "Usage: nameget <name>\n" + "  name: Name of the entry to get";
    }

    @Override
    public void exec(final String[] p_args, final TerminalCommandContext p_ctx) {
        String name = TerminalCommandContext.getArgString(p_args, 0, null);

        if (name == null) {
            TerminalCommandContext.printlnErr("No name specified");
            return;
        }

        NameserviceService nameservice = p_ctx.getService(NameserviceService.class);

        long cid = nameservice.getChunkID(name, 2000);

        if (cid == ChunkID.INVALID_ID) {
            TerminalCommandContext.printflnErr("Could not get name entry for %s, does not exist", name);
        } else {
            TerminalCommandContext.printfln("%s: 0x%X", name, cid);
        }
    }
}
