function help() {
	return "Remove an existing chunk. Usable with either full chunk id or split into nid and lid\n" +
			"Parameters: cidStr | nid lid\n" +
			"  cidStr: Full chunk id of the chunk to remove as string\n" +
			"  nid: Node id to remove the chunk with specified local id\n" +
			"  lid: Local id of the chunk to remove. If missing node id, current node is assumed";
}

function exec(id1, id2) {

    if (id1 == null) {
        dxterm.printlnErr("No cid or nid specified")
        return
    }

    if (id2 == null) {
        execCid(id1)
    } else {
        execNidLid(dxram.cid(id1, id2))
    }
}

function execCid(cidStr) {
    if (cidStr == null) {
        dxterm.printlnErr("No cid specified");
        return;
    }

    var cid = dxram.cid(cidStr)

    // don't allow removal of index chunk
    if (dxram.lidOfCid(cid) == 0) {
        dxterm.printlnErr("Removal of index chunk is not allowed.")
        return;
    }

    var chunk = dxram.service("chunk");

    if (chunk.remove(cid) != 1) {
        dxterm.printlnErr("Removing chunk with ID " + dxram.cidHexStr(cid) + " failed.");
    } else {
        dxterm.println("Chunk " + dxram.cidHexStr(cid) + " removed.");
    }
}

function execNidLid(nid, lid) {

}
