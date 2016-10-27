function imports() {

}

function help() {
	return "Get information about either the current node or another node in the network\n" +
			"Parameters: nid\n" +
			"  nid: If specified, gets information of this node";
}

function exec(nid) {
	var boot = dxram.service("boot");

	if (nid != null) {
		if (boot.nodeAvailable(nid)) {
			dxterm.println("Node info 0x%X:", nid);
			dxterm.println("\tRole: %s", boot.getNodeRole(nid));
			dxterm.println("\tAddress: %s", boot.getNodeAddress(nid));
		} else {
			dxterm.println("Not available.");
		}
	} else {
		dxterm.println("Node info 0x%X:", boot.getNodeID());
		dxterm.println("\tRole: %s", boot.getNodeRole());
		dxterm.println("\tAddress: %s", boot.getNodeAddress(boot.getNodeID()));
	}
}
