function help() {
	return "Shutdown a DXRAM node\n" +
			"Parameters: nid kill\n" +
			"  nid: Id of the node to shutdown\n" +
			"  kill: If specified, true for a hard shutdown (kill process), false for proper soft shutdown (default)"
}

function exec(nid, kill) {
	var boot = dxram.service("boot")

	if (nid == null) {
		print("No nodeID specified")
		return
	}

	if (kill == null) {
		kill = false
	}

	if (!boot.shutdownNode(nid, kill)) {
		print("Shutting down node " + dxram.nidhexstr(nid) + " failed")
	} else {
		print("Shutting down node " + dxram.nidhexstr(nid) + "...")
	}
}
