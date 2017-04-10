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

package de.hhu.bsinfo.dxram.tmp.tcmd;

import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalCommandContext;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;

/**
 * Remove a (stored) chunk from temporary storage (superpeer storage)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdTmpremove extends AbstractTerminalCommand {
    public TcmdTmpremove() {
        super("tmpremove");
    }

    @Override
    public String getHelp() {
        return "Remove a (stored) chunk from temporary storage (superpeer storage)\n" + "Usage: tmpremove <id>\n" +
            "  id: Id of the chunk in temporary storage";
    }

    @Override
    public void exec(final String[] p_args, final TerminalCommandContext p_ctx) {
        int id = p_ctx.getArgInt(p_args, 0, -1);

        if (id == -1) {
            p_ctx.printlnErr("No id specified");
            return;
        }

        TemporaryStorageService tmpstore = p_ctx.getService(TemporaryStorageService.class);

        if (tmpstore.remove(id)) {
            p_ctx.printfln("Removed chunk with id 0x%X from temporary storage", id);
        } else {
            p_ctx.printlnErr("Creating chunk in temporary storage failed");
        }
    }
}
