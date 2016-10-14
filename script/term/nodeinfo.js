function help() {
	return "Get information about either the current node or another node in the network"
}

function exec(nodeID) {
	var boot = dxram.service("boot")

	print("Node info " + dxram.nodeIdToHexString(nodeID) + ":")
	if (nodeID) {
		if (boot.nodeAvailable(nodeID)) {
			print("\tRole: " + boot.getNodeRole(nodeID));
			print("\tAddress: " + boot.getNodeAddress(nodeID));
		} else {
			print("Not available.");
		}
	} else {
		print("\tRole: " + boot.getNodeRole())
		print("\tAddress: " + boot.getNodeAddress(boot.getNodeID()))
	}
}
