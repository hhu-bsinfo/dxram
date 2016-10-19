function imports() {

}

function help() {

	return "Recovers all data of owner on dest\n" +
			"Parameters: ownerNid destNid\n" +
			"  ownerNid: Node id of the owner to recover data of\n" +
			"  destNid: Destination node id to recover the data to";
}

function exec(ownerNid, destNid) {

    if (ownerNid == null) {
        dxterm.printlnErr("No ownerNid specified");
        return;
    }

    if (destNid == null) {
        dxterm.printlnErr("No destNid specified");
        return;
    }

    dxram.service("recovery").recover(ownerNid, destNid, true);
}
