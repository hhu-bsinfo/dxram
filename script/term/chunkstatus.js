function help() {
	return "Get the status of the chunk service/memory from a remote node\n" +
			"Parameters: nid sizetype\n" +
			"  nid: Node ID of the remote peer to get the status from\n" +
			"  sizetype: Specify the type of size you want to display (b, kb, mb, gb)";
}

function exec(nid, sizetype) {

    if (nid == null) {
        dxterm.printlnErr("No nid specified");
        return;
    }

    if (sizetype == null) {
        sizetype = "b";
    }

    var chunk = dxram.service("chunk");

    var status = chunk.getStatus(nid);

    if (status == null) {
        dxterm.printlnErr("Getting status failed");
        return;
    }

    var divisor = 1;
    if (sizetype != null) {
        sizetype = sizetype.toLowerCase();
        if (sizetype.equals("b")) {
            divisor = 1;
        } else if (sizetype.equals("kb")) {
            divisor = 1024;
        } else if (sizetype.equals("mb")) {
            divisor = 1024 * 1024;
        } else if (sizetype.equals("gb")) {
            divisor = 1024 * 1024 * 1024;
        } else {
            // invalid, default to byte
            sizetype = "b";
        }
    } else {
        sizetype = "b";
    }

    dxterm.println("Chunk service/memory status of " + dxram.shortToHexStr(nid) + ":");
    if (divisor == 1) {
        dxterm.println("Free memory (" + sizetype + "): " + status.getFreeMemory());
        dxterm.println("Total memory (" + sizetype + "): " + status.getTotalMemory());
        dxterm.println("Total payload memory (" + sizetype + "): " + status.getTotalPayloadMemory());
        dxterm.println("Total chunk payload memory (" + sizetype + "): " + status.getTotalChunkPayloadMemory());
        dxterm.println("Total CID tables memory (NID table with 327687) (" + sizetype + "): "
                        + status.getTotalMemoryCIDTables());
    } else {
        dxterm.println("Free memory (" + sizetype + "): " + status.getFreeMemory() / divisor);
        dxterm.println("Total memory (" + sizetype + "): " + status.getTotalMemory() / divisor);
        dxterm.println("Total payload memory (" + sizetype + "): " + status.getTotalPayloadMemory() / divisor);
        dxterm.println("Total chunk payload memory (" + sizetype + "): "
                        + status.getTotalChunkPayloadMemory() / divisor);
        dxterm.println("Total CID tables memory (NID table with 327687) (" + sizetype + "): "
                        + status.getTotalMemoryCIDTables() / divisor);
    }

    dxterm.println("Num active memory blocks: " + status.getNumberOfActiveMemoryBlocks());
    dxterm.println("Num active chunks: " + status.getNumberOfActiveChunks());
    dxterm.println("Num CID tables (one is NID table): " + status.getCIDTableCount());
}
