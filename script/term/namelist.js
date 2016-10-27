function imports() {

}

function help() {

	return "List all registered name mappings of the nameservice";
}

function exec() {

    var nameservice = dxram.service("name");
    var entries = nameservice.getAllEntries();

    dxterm.printfln("Nameservice entries(%d):", entries.size());

    for each (entry in entries) {
        dxterm.printfln("%s: 0x%X", entry.first(), entry.second());
    }
}
