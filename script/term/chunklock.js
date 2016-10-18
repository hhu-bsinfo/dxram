function help() {
	return "Lock a chunk\n" +
			"Parameters: cidStr | nid lid\n" +
			"  cidStr: Full chunk ID of the chunk to lock as string\n" +
			"  nid: Separate local id part of the chunk to lock\n" +
			"  lid: Separate node id part of the chunk to lock";
}

function exec(id1, id2) {

    if (id1 == null) {
        dxterm.printlnErr("No cid or nid specified");
        return;
    }

    if (id2 == null) {
        execCid(dxram.longStrToLong(id1));
    } else {
        execCid(dxram.cid(id1, id2));
    }
}

function execCid(cid) {
    if (cid == null) {
        dxterm.printlnErr("No cid specified");
        return;
    }

    // don't allow lock of index chunk
    if (dxram.lidOfCid(cid) == 0) {
        dxterm.printlnErr("Locking of index chunk is not allowed.")
        return;
    }

    var err = dxram.service("lock").lock(true, 1000, cid);
    if (!err.toString().equals("SUCCESS")) {
        dxterm.printlnErr("Error locking chunk " + dxram.longToHexStr(cid) + ": " + err);
    } else {
        dxterm.println("Locked chunk " + dxram.longToHexStr(cid));
    }
}
