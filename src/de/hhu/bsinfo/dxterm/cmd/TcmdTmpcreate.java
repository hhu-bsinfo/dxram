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

import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;

/**
 * Allocate memory for a chunk on a superpeer's storage (temporary)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdTmpcreate extends AbstractTerminalCommand {
    public TcmdTmpcreate() {
        super("tmpcreate");
    }

    @Override
    public String getHelp() {
        return "Allocate memory for a chunk on a superpeer's storage (temporary)\n" + "Usage: tmpcreate <id> <size>\n" +
                "  id: Id to identify the chunk in the storage\n" + "  size: Size of the chunk to create";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServiceAccessor p_services) {
        int id = p_cmd.getArgInt(0, -1);
        int size = p_cmd.getArgInt(1, -1);

        if (id == -1) {
            p_stdout.printlnErr("No id specified");
            return;
        }

        if (size == -1) {
            p_stdout.printlnErr("No size specified");
            return;
        }

        TemporaryStorageService tmpstore = p_services.getService(TemporaryStorageService.class);

        if (tmpstore.create(id, size)) {
            p_stdout.printfln("Created chunk of size %d in temporary storage: 0x%d", size, id);
        } else {
            p_stdout.printlnErr("Creating chunk in temporary storage failed");
        }
    }
}
