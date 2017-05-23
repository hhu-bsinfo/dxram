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

import java.util.List;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;

/**
 * Wait for a minimum number of nodes to be online
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdNodewait extends AbstractTerminalCommand {
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
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServiceAccessor p_services) {
        int superpeers = p_cmd.getArgInt(0, 0);
        int peers = p_cmd.getArgInt(1, 0);
        int pollIntervalMs = p_cmd.getArgInt(2, 1000);

        BootService boot = p_services.getService(BootService.class);

        p_stdout.printfln("Waiting for at least %d superpeer(s) and %d peer(s)...", superpeers, peers);

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

        p_stdout.printfln("%d superpeers and %d peers online", listSuperpeers.size(), listPeers.size());
    }
}
