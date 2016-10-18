function help() {
	return "Prints the log utilization of given peer\n" +
			"Parameters: nid\n" +
			"  nid: Node id of the peer";
}

function exec(nid) {

    if (nid == null) {
        dxterm.printlnErr("No nid specified");
        return;
    }

    var log = dxram.service("log");

    dxterm.println(log.getCurrentUtilization(nid));
}
