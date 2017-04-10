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

package de.hhu.bsinfo.dxram.sync.tcmd;

import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalCommandContext;

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
    public void exec(final String[] p_args, final TerminalCommandContext p_ctx) {
        int size = TerminalCommandContext.getArgInt(p_args, 0, -1);

        if (size == -1) {
            TerminalCommandContext.printlnErr("No size specified");
            return;
        }

        SynchronizationService sync = p_ctx.getService(SynchronizationService.class);

        int barrierId = sync.barrierAllocate(size);
        if (barrierId == BarrierID.INVALID_ID) {
            TerminalCommandContext.printlnErr("Allocating barrier failed");
        } else {
            TerminalCommandContext.printfln("Allocating barrier successful, barrier id: 0x%X", barrierId);
        }
    }
}
