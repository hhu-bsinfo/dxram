function imports() {

}

function help() {
	return "Wait for a minimum number of nodes to be available/online\n" +
			"Parameters: superpeers peers pollIntervalMs\n" +
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
