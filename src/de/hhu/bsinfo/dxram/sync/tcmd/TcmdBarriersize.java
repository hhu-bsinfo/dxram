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
 * Change the size of an existing barrier
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdBarriersize extends AbstractTerminalCommand {
    public TcmdBarriersize() {
        super("barriersize");
    }

    @Override
    public String getHelp() {
        return "Change the size of an existing barrier\n" + "Usage: barriersize <bid> <size>\n" + "  bid: Id of the barrier to change its size\n" +
            "  size: New size for the existing barrier";
    }

    @Override
    public void exec(final String[] p_args, final TerminalCommandContext p_ctx) {
        int bid = TerminalCommandContext.getArgBarrierId(p_args, 0, BarrierID.INVALID_ID);
        int size = TerminalCommandContext.getArgInt(p_args, 1, -1);

        if (bid == BarrierID.INVALID_ID) {
            TerminalCommandContext.printlnErr("No bid specified");
            return;
        }

        if (size == -1) {
            TerminalCommandContext.printlnErr("No size specified");
            return;
        }

        SynchronizationService sync = p_ctx.getService(SynchronizationService.class);

        if (!sync.barrierChangeSize(bid, size)) {
            TerminalCommandContext.printflnErr("Changing barrier 0x%X size to %d failed", bid, size);
        } else {
            TerminalCommandContext.printfln("Barrier 0x%X size changed to %d", bid, size);
        }
    }
}
