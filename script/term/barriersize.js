function help() {

	return "Change the size of an existing barrier, keeping its id\n" +
			"Parameters: bid size\n" +
			"  bid: Id of the barrier to change its size\n" +
			"  size: New size for the existing barrier";
}

function exec(bid, size) {

    if (bid == null) {
        dxterm.printlnErr("No bid specified");
        return;
    }

    if (size == null) {
        dxterm.printlnErr("No size specified");
        return;
    }

    var sync = dxram.service("sync");

    if (!sync.barrierChangeSize(bid, size)) {
        dxterm.printlnErr("Changing barrier " + dxram.bidHexStr(bid) + " size to " + size + " failed.");
    } else {
        dxterm.println("Barrier " + dxram.bidHexStr(bid) + " size changed to " + size);
    }
}
