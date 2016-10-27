function imports() {

}

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
        dxterm.printflnErr("Could not get name entry for %s, does not exist", name);
    } else {
        dxterm.printfln("%s: 0x%X", name, cid);
    }
}
