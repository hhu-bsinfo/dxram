function imports() {

}

function help() {

	return "Remove a (stored) chunk from temporary storage (superpeer storage)\n" +
			"Parameters: id\n" +
			"  id: Id of the chunk in temporary storage";
}

function exec(id) {

    if (id == null) {
        dxterm.printlnErr("No id specified");
        return;
    }

    var tmpstore = dxram.service("tmpstore");

    if (tmpstore.remove(id)) {
        dxterm.println("Removed chunk with id " + dxram.longToHexStr(id) + " from temporary storage.");
    } else {
        dxterm.printlnErr("Creating chunk in temporary storage failed");
    }
}
