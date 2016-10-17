function help() {
	return "List all available nodes or nodes of a specific type\n" +
			"Parameters: role\n" +
			"  role: Filter list by role if specified"
}

function exec(role) {

	var boot = dxram.service("boot")
	var nodeRole = dxram.noderole(role)

	var nodeIds = boot.getOnlineNodeIDs()

	if (role != null) {
		print("Filtering by role " + nodeRole)
	} 

	print("Total available nodes (" + nodeIds.size() + "):")
		
	for each(nodeId in nodeIds) {
		var curRole = boot.getNodeRole(nodeId)
		
		if (role != null || role == null && nodeRole.equals(curRole)) {
			print("\t" + dxram.nidhexstr(nodeId) + ", " + curRole + ", " + boot.getNodeAddress(nodeId))
		}
	}
}
