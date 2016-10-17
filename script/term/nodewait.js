function help() {
	return "Wait for a minimum number of nodes to be available/online\n" +
			"Parameters: superpeers peers pollIntervalMs\n" +
			"  superpeers: Number of available superpeers to wait for (default 0)\n" +
			"  peers: Number of available peers to wait for (default 0)\n" +
			"  pollIntervalMs: Polling interval when checking online status (default 1000)"
}

function exec(superpeers, peers, pollIntervalMs) {

	if (!superpeers) {
		superpeers = 0
	}
	
	if (!peers) {
		peers = 0
	}

	if (!pollIntervalMs) {
		pollIntervalMs = 1000
	}

	var boot = dxram.service("boot")

	print("Waiting for at least " + superpeers + " superpeer(s) and " + peers + " peer(s)...")

	var listSuperpeers = boot.getOnlineSuperpeerNodeIDs()
	while (listSuperpeers.size() < superpeers) {
		dxram.sleep(pollIntervalMs)

		listSuperpeers = boot.getOnlineSuperpeerNodeIDs();
	}

	var listPeers = boot.getOnlinePeerNodeIDs()
	while (listPeers.size() < peers) {
		dxram.sleep(pollIntervalMs)

		listPeers = boot.getOnlinePeerNodeIDs();
	}

	print(listSuperpeers.size() + " superpeers and " + listPeers.size() + " peers online")
}
