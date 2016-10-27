function imports() {

}

function help() {
	return "Prints the look up tree of a specified node\n" +
			"Parameters: nid\n" +
			"  nid: Node id of the peer to print the lookup tree of";
}

function exec(nid) {

    if (nid == null) {
        dxterm.printlnErr("No nid specified");
        return;
    }

    var lookup = dxram.service("lookup");

    var respSuperpeer = lookup.getResponsibleSuperpeer(nid);

    if (respSuperpeer == -1) {
        dxterm.printflnErr("No responsible superpeer for 0x%X found", nid);
        return;
    }

    var tree = lookup.getLookupTreeFromSuperpeer(respSuperpeer, nid);
    dxterm.printfln("Lookup tree of 0x%X:\n%s", nid, tree);
}
