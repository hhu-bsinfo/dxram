function imports() {

}

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
        dxterm.printflnErr("Changing barrier 0x%X size to %d failed", bid, size);
    } else {
        dxterm.printfln("Barrier 0x%X size changed to %d", bid, size);
    }
}
