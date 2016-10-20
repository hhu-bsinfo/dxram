function imports() {

}

function help() {

	return "Get a list of available compute groups"
}

function exec(nid) {

    var mscomp = dxram.service("mscomp");
    var masters = mscomp.getMasters();

    dxterm.println("List of available compute groups with master nodes (" + masters.size() + "):")
    for each (master in masters) {
        dxterm.println(master.second() + ": " + dxram.shortToHexStr(entry.first()));
    }
}
