function imports() {

}

function help() {
	return "Get the list of all locked chunks of a node\n" +
			"Parameters: nid\n" +
			"  nid: Get the list of locked chunks from a remote node";
}

function exec(nid, id2) {

    if (nid == null) {
        dxterm.printlnErr("No nid specified");
        return;
    }

    var boot = dxram.service("boot");
    var lock = dxram.service("lock");

    var list = lock.getLockedList(nid);

    if (list == null) {
       dxterm.printlnErr("Getting list of locked chunks failed");
       return;
    }

    dxterm.printfln("Locked chunks of 0x%X (%d):", nid, list.size());
    dxterm.println("<lid: nid that locked the chunk>");
    for each (entry in list) {
        dxterm.printfln("0x%X: 0x%X", entry.first(), entry.second());
    }
}
