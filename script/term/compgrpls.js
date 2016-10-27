function imports() {

}

function help() {

	return "Get a list of available compute groups"
}

function exec(nid) {

    var mscomp = dxram.service("mscomp");
    var masters = mscomp.getMasters();

    dxterm.printfln("List of available compute groups with master nodes (%d):", masters.size())
    for each (master in masters) {
        dxterm.printfln("%d: 0x%X", master.second(), entry.first());
    }
}
