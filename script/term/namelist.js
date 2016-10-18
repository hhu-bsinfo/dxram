function help() {

	return "List all registered name mappings of the nameservice";
}

function exec() {

    var nameservice = dxram.service("name");
    var entries = nameservice.getAllEntries();

    dxterm.println("Nameservice entries(" + entries.size() + "):");

    for each (entry in entries) {
        dxterm.println(entry.first() + ": " + dxram.longToHexStr(entry.second()));
    }
}
