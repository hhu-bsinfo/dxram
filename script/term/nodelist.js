function help() {
	return "List all available nodes or nodes of a specific type\n" +
			"Parameters: role\n" +
			"  role: Filter list by role if specified";
}

function exec(role) {

	var boot = dxram.service("boot");
	var nodeRole = dxram.nodeRole(role);

	var nodeIds = boot.getOnlineNodeIDs();

	if (role != null) {
		dxterm.println("Filtering by role " + nodeRole);
	} 

	dxterm.println("Total available nodes (" + nodeIds.size() + "):");
		
	for each(nodeId in nodeIds) {
		var curRole = boot.getNodeRole(nodeId);
		
		if (role == null || role != null && nodeRole.equals(curRole)) {
			dxterm.println("\t" + dxram.shortToHexStr(nodeId) + ", " + curRole + ", " + boot.getNodeAddress(nodeId));
		}
	}
}
