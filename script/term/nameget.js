function help() {

	return "Register a nameservice entry for a specific chunk id\n" +
			"Parameters: name\n" +
			"  name: Name to get the chunk id for";
}

function exec(name) {

    if (name == null) {
        dxterm.printlnErr("No name specified");
        return;
    }

    var nameservice = dxram.service("name");

    var cid = nameservice.getChunkID(name, 2000);

    if (cid == -1) {
        dxterm.printlnErr("Could not get name entry for " + name + ", does not exist");
    } else {
        dxterm.println(name + ": " + dxram.longToHexStr(cid));
    }
}
