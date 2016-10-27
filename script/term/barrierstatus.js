function imports() {

}

function help() {

	return "Get the current status of a barrier\n" +
			"Parameters: bid\n" +
			"  bid: Id of an allocated barrier to get the status of";
}

function exec(bid) {

    if (bid == null) {
        dxterm.printlnErr("No bid specified");
        return;
    }

    var sync = dxram.service("sync");
    var status = sync.barrierGetStatus(bid);

    if (status == null) {
        dxterm.printflnErr("Getting status of barrier 0x%X failed", bid);
        return;
    }

    var peers = "";
    for (var i = 1; i < status.length; i++) {
        peers += dxram.shortToHexStr(status[i]) + ", ";
    }

    dxterm.printfln("Barrier status 0x%X, %s/%d: %d", bid, status[0], (status.length - 1), peers);
}
