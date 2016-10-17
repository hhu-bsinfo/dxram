function help() {
	return "Get information about either the current node or another node in the network\n" +
			"Parameters: nid\n" +
			"  nid: If specified, gets information of this node";
}

function exec(nid) {
	var boot = dxram.service("boot");

	if (nid != null) {
		if (boot.nodeAvailable(nid)) {
			dxterm.println("Node info " + dxram.nidhexstr(nid) + ":");
			dxterm.println("\tRole: " + boot.getNodeRole(nid));
			dxterm.println("\tAddress: " + boot.getNodeAddress(nid));
		} else {
			dxterm.println("Not available.");
		}
	} else {
		dxterm.println("Node info " + dxram.nidhexstr(boot.getNodeID()) + ":");
		dxterm.println("\tRole: " + boot.getNodeRole());
		dxterm.println("\tAddress: " + boot.getNodeAddress(boot.getNodeID()));
	}
}
