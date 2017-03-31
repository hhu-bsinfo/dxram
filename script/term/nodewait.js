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

function imports() {

}

function help() {
	return "Wait for a minimum number of nodes to be available/online\n" +
			"Usage (1): nodewait()\n" +
			"Usage (2): nodewait(superpeers)\n" +
			"Usage (3): nodewait(superpeers, peers)\n" +
			"Usage (4): nodewait(superpeers, peers, pollIntervalMs)\n" +
			"  superpeers: Number of available superpeers to wait for (default 0)\n" +
			"  peers: Number of available peers to wait for (default 0)\n" +
			"  pollIntervalMs: Polling interval when checking online status (default 1000)";
}

function exec(superpeers, peers, pollIntervalMs) {

	if (superpeers == null) {
		superpeers = 0;
	}

	if (peers == null) {
		peers = 0;
	}

	if (pollIntervalMs == null) {
		pollIntervalMs = 1000;
	}

	var boot = dxram.service("boot");

	dxterm.printfln("Waiting for at least %d superpeer(s) and %d peer(s)...", superpeers, peers);

	var listSuperpeers = boot.getOnlineSuperpeerNodeIDs();
	while (listSuperpeers.size() < superpeers) {
		dxram.sleep(pollIntervalMs);

		listSuperpeers = boot.getOnlineSuperpeerNodeIDs();
	}

	var listPeers = boot.getOnlinePeerNodeIDs()
	while (listPeers.size() < peers) {
		dxram.sleep(pollIntervalMs);

		listPeers = boot.getOnlinePeerNodeIDs();
	}

	dxterm.printfln("%d superpeers and %d peers online", listSuperpeers.size(), listPeers.size());
}
