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

package de.hhu.bsinfo.dxram.boot.tcmd;

import java.util.List;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalCommandContext;

/**
 * Wait for a minimum number of nodes to be online
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdNodewait extends TerminalCommand {
    public TcmdNodewait() {
        super("nodewait");
    }

    @Override
    public String getHelp() {
        return "Wait for a minimum number of nodes to be online\n" + "Usage: nodewait [superpeers] [peers] [pollIntervalMs]\n" +
            "  superpeers: Number of available superpeers to wait for (default 0)\n" + "  peers: Number of available peers to wait for (default 0)\n" +
            "  pollIntervalMs: Polling interval when checking online status (default 1000)";
    }

    @Override
    public void exec(final String[] p_args, final TerminalCommandContext p_ctx) {
        int superpeers = p_ctx.getArgInt(p_args, 0, 0);
        int peers = p_ctx.getArgInt(p_args, 1, 0);
        int pollIntervalMs = p_ctx.getArgInt(p_args, 2, 1000);

        BootService boot = p_ctx.getService(BootService.class);

        p_ctx.printfln("Waiting for at least %d superpeer(s) and %d peer(s)...", superpeers, peers);

        List<Short> listSuperpeers = boot.getOnlineSuperpeerNodeIDs();
        while (listSuperpeers.size() < superpeers) {
            try {
                Thread.sleep(pollIntervalMs);
            } catch (final InterruptedException ignored) {

            }

            listSuperpeers = boot.getOnlineSuperpeerNodeIDs();
        }

        List<Short> listPeers = boot.getOnlinePeerNodeIDs();
        while (listPeers.size() < peers) {
            try {
                Thread.sleep(pollIntervalMs);
            } catch (final InterruptedException ignored) {

            }

            listPeers = boot.getOnlinePeerNodeIDs();
        }

        p_ctx.printfln("%d superpeers and %d peers online", listSuperpeers.size(), listPeers.size());
    }
}
