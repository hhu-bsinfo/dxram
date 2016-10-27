function imports() {

}

function help() {
	return "Create a chunk on a remote node\n" +
			"Parameters: size nid\n" +
			"  size: Size of the chunk to create\n" +
			"  nid: Node id of the peer to create the chunk on";
}

function exec(size, nid) {

	if (size == null) {
		dxterm.printlnErr("No size specified");
		return;
	}
	
	if (nid == null) {
		dxterm.printlnErr("No nid specified");
		return;
	}

	var chunk = dxram.service("chunk");

	var chunkIDs = chunk.createRemote(nid, size);

	if (chunkIDs != null) {
	    dxterm.printfln("Created chunk of size %d: 0x%X", size, chunkIDs[0]);
	} else {
        dxterm.printlnErr("Creating chunk failed");
	}
}
