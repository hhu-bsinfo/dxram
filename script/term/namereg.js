function help() {

	return "Get the chunk id for a registered name mapping\n" +
			"Parameters: cidStr or nid|lid name\n" +
            "  cidStr: Full chunk ID of the chunk to register as string\n" +
            "  nid: Separate local id part of the chunk to register\n" +
            "  lid: Separate node id part of the chunk to register\n" +
            "  name: Name to register the chunk id for";
}

function exec(id1, id2, name) {

    if (id1 == null) {
        dxterm.printlnErr("No cid or nid specified");
        return;
    }

    if (name == null) {
        dxterm.printlnErr("No name specified");
        return;
    }

    if (id2 == null) {
        execCid(dxram.longStrToLong(id1), name);
    } else {
        execCid(dxram.cid(id1, id2), name);
    }
}

function execCid(cid, name) {

    dxram.service("name").register(cid, name);
}
