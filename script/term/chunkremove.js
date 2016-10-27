function imports() {

}

function help() {
	return "Remove an existing chunk. Usable with either full chunk id or split into nid and lid\n" +
			"Parameters: cidStr | nid lid\n" +
			"  cidStr: Full chunk id of the chunk to remove as string\n" +
			"  nid: Node id to remove the chunk with specified local id\n" +
			"  lid: Local id of the chunk to remove. If missing node id, current node is assumed";
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

    // don't allow removal of index chunk
    if (dxram.lidOfCid(cid) == 0) {
        dxterm.printlnErr("Removal of index chunk is not allowed.=")
        return;
    }

    var chunk = dxram.service("chunk");

    if (chunk.remove(cid) != 1) {
        dxterm.printflnErr("Removing chunk with ID 0x%X failed", cid);
    } else {
        dxterm.printfln("Chunk 0x%X removed", cid);
    }
}
