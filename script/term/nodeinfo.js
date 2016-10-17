function help() {
	return "Get information about either the current node or another node in the network\n" +
			"Parameters: nid\n" +
			"  nid: If specified, gets information of this node"
}

function exec(nid) {
	var boot = dxram.service("boot")

	if (nid) {
		if (boot.nodeAvailable(nid)) {
			print("Node info " + dxram.nidhexstr(nid) + ":")
			print("\tRole: " + boot.getNodeRole(nid));
			print("\tAddress: " + boot.getNodeAddress(nid));
		} else {
			print("Not available.");
		}
	} else {
		print("Node info " + dxram.nidhexstr(boot.getNodeID()) + ":")
		print("\tRole: " + boot.getNodeRole())
		print("\tAddress: " + boot.getNodeAddress(boot.getNodeID()))
	}
}
