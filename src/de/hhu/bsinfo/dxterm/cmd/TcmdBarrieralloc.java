/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxterm.cmd;

import java.util.Collections;
import java.util.List;

import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdin;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;

/**
 * Create a new barrier for synchronization of mutliple peers
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdBarrieralloc extends AbstractTerminalCommand {
    public TcmdBarrieralloc() {
        super("barrieralloc");
    }

    @Override
    public String getHelp() {
        return "Create a new barrier for synchronization of mutliple peers\n" + "Usage: barrieralloc <size>\n" +
                "  size: Size of the barrier, i.e. the number of peers that have to sign on for release";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServerStdin p_stdin,
            final TerminalServiceAccessor p_services) {
        int size = p_cmd.getArgInt(0, -1);

        if (size == -1) {
            p_stdout.printlnErr("No size specified");
            return;
        }

        SynchronizationService sync = p_services.getService(SynchronizationService.class);

        int barrierId = sync.barrierAllocate(size);
        if (barrierId == BarrierID.INVALID_ID) {
            p_stdout.printlnErr("Allocating barrier failed");
        } else {
            p_stdout.printfln("Allocating barrier successful, barrier id: 0x%X", barrierId);
        }
    }

    @Override
    public List<String> getArgumentCompletionSuggestions(final int p_argumentPos, final TerminalCommandString p_cmdStr,
            final TerminalServiceAccessor p_services) {
        return Collections.emptyList();
    }
}
