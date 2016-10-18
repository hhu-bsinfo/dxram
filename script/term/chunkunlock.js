function help() {
	return "Unlock a previously locked chunk\n" +
			"Parameters: cidStr | nid lid\n" +
			"  cidStr: Full chunk ID of the chunk to unlock as string\n" +
			"  nid: Separate local id part of the chunk to unlock\n" +
			"  lid: Separate node id part of the chunk to unlock";
}

function exec(id1, id2) {

    if (id1 == null) {
        dxterm.printlnErr("No cid or nid specified");
        return;
    }

    if (id2 == null) {
        execCid(dxram.cid(id1));
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
        dxterm.printlnErr("Locking/Unlocking of index chunk is not allowed.")
        return;
    }

    var err = dxram.service("lock").unlock(true, cid);
    if (!err.toString().equals("SUCCESS")) {
        dxterm.printlnErr("Error unlocking chunk " + dxram.cidHexStr(cid) + ": " + err);
    } else {
        dxterm.println("Unlocked chunk " + dxram.cidHexStr(cid));
    }
}
