function imports() {

}

function help() {

	return "Sign on to an allocated barrier for synchronization (for testing/debugging)\n" +
			"Parameters: bid [data]\n" +
			"  bid: Id of the barrier to sign on to\n" +
			"  data: Custom data to pass along with the sign on call (optional)";
}

function exec(bid, data) {

    if (bid == null) {
        dxterm.printlnErr("No bid specified");
        return;
    }

    if (data == null) {
        data = 0;
    }

    var sync = dxram.service("sync");
    var result = sync.barrierSignOn(bid, data);

    if (result == null) {
        dxterm.printlnErr("Signing on to barrier " + dxram.intToHexStr(bid) + " failed.");
        return;
    }

    var str = "";
    for (var i = 0; i < result.first().length; i++) {
        str += "\n" + dxram.shortToHexStr(result.first()[i]) + ": " + dxram.longToHexStr(result.second()[i]);
    }

    dxterm.println("Synchronized to barrier " + dxram.intToHexStr(bid) + " custom data: " + str)
}
