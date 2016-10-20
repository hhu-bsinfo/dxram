function imports() {

}

function help() {

	return "Create a new barrier for synchronization of mutliple peers\n" +
			"Parameters: size\n" +
			"  size: Size of the barrier, i.e. the number of peers that have to sign on for release";
}

function exec(size) {

    if (size == null) {
        dxterm.printlnErr("No size specified");
        return;
    }

    var sync = dxram.service("sync");

    var barrierId = sync.barrierAllocate(size);
    if (barrierId == -1) {
        dxterm.printlnErr("Allocating barrier failed.");
    } else {
        dxterm.println("Allocating barrier successful, barrier id: " + dxram.intToHexStr(barrierId));
    }
}
