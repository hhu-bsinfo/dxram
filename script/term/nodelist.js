function imports() {

}

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
		dxterm.printfln("Filtering by role %s", nodeRole);
	} 

	dxterm.printfln("Total available nodes (%d):", nodeIds.size());
		
	for each(nodeId in nodeIds) {
		var curRole = boot.getNodeRole(nodeId);
		
		if (role == null || role != null && nodeRole.equals(curRole)) {
			dxterm.printfln("\t0x%X, %s, %s", nodeId, curRole, nodeId);
		}
	}
}
