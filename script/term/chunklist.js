function imports() {

}

function help() {
	return "Get a list of chunk id ranges from a peer holding chunks (migrated chunks optional)\n" +
			"Parameters: nid migrated\n" +
			"  nid: Node ID of the remote peer to get the list from\n" +
			"  migrated: List the migrated chunks as well (default: false)";
}

function exec(nid, migrated) {

	if (nid == null) {
		dxterm.printlnErr("No nid specified");
		return;
	}

	if (migrated == null) {
		migrated = false;
	}

	var chunk = dxram.service("chunk");

	var chunkRanges = chunk.getAllLocalChunkIDRanges(nid);

    if (chunkRanges == null) {
        dxterm.printlnErr("Getting chunk ranges failed.");
        return;
    }

    dxterm.println("Locally created chunk id ranges of " + dxram.shortToHexStr(nid) + "(" + chunkRanges.size() / 2 + "):");

    for (var i = 0; i < chunkRanges.size(); i++) {
        var currRange = chunkRanges.get(i);
        if (i % 2 == 0) {
            dxterm.print("[" + dxram.longToHexStr(currRange));
        } else {
            dxterm.println(", " + dxram.longToHexStr(currRange) + "]");
        }
    }

    if (migrated) {

        chunkRanges = chunk.getAllMigratedChunkIDRanges(nid);

        if (chunkRanges == null) {
            dxterm.printlnErr("Getting migrated chunk ranges failed.");
            return;
        }

        dxterm.println("Migrated chunk id ranges of " + dxram.shortToHexStr(nid) + "(" + chunkRanges.size() / 2 + "):");

        for (var i = 0; i < chunkRanges.size(); i++) {
            var currRange = chunkRanges.get(i);
            if (i % 2 == 0) {
                dxterm.print("[" + dxram.longToHexStr(currRange));
            } else {
                dxterm.println(", " + dxram.longToHexStr(currRange) + "]");
            }
        }
    }
}
