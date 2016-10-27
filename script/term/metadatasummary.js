function imports() {

}

function help() {
	return "Prints a summary of the specified superpeer's metadata\n" +
			"Parameters: nid\n" +
			"  nid: Node id of the superpeer to print the metadata of\n" +
			"       \"all\" prints metadata of all superpeers";
}

function exec(nid) {

    if (nid == null) {
        dxterm.printlnErr("No nid specified");
        return;
    }

    var lookup = dxram.service("lookup");
    if (nid == "all") {
        var boot = dxram.service("boot");
        var nodeIds = boot.getOnlineNodeIDs();

        for each(nodeId in nodeIds) {
            var curRole = boot.getNodeRole(nodeId);
            if (curRole == "superpeer") {
                var summary = lookup.getMetadataSummary(nodeId);
                dxterm.printfln("Metadata summary of 0x%X:\n%s", nodeId, summary);
            }
        }
    } else {
        var summary = lookup.getMetadataSummary(nid);
        dxterm.printfln("Metadata summary of 0x%X:\n%s", nid, summary);
    }
}
