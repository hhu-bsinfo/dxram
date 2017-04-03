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

import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalCommandContext;

/**
 * Create a new barrier for synchronization of mutliple peers
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdBarrierfree extends TerminalCommand {
    public TcmdBarrierfree() {
        super("barrierfree");
    }

    @Override
    public String getHelp() {
        return "Free an allocated barrier\n" +
                "Usage: barrierfree <bid>\n" +
                "  bid: Id of an allocated barrier to free";
    }

    @Override
    public void exec(final String[] p_args, final TerminalCommandContext p_ctx) {
        int bid = p_ctx.getArgBarrierId(p_args, 0, BarrierID.INVALID_ID);

        if (bid == BarrierID.INVALID_ID) {
            p_ctx.printlnErr("No bid specified");
            return;
        }

        SynchronizationService sync = p_ctx.getService(SynchronizationService.class);

        if (!sync.barrierFree(bid)) {
            p_ctx.printlnErr("Freeing barrier failed");
        } else {
            p_ctx.printfln("Barrier 0x%X free'd", bid);
        }
    }
}
