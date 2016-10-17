function help() {
	return "Reboot a DXRAM node\n" +
			"Parameters: nid\n" +
			"  nid: Id of the node to reboot"
}

function exec(nid) {
	var boot = dxram.service("boot")

	if (!nid) {
		print("No nodeID specified")
		return
	} 

	if (!boot.rebootNode(nid)) {
		print("Rebooting node " + dxram.nidhexstr(nid) + " failed")
	} else {
		print("Rebooting node " + dxram.nidhexstr(nid) + "...")
	}
}
