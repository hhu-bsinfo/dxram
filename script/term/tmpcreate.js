function imports() {

}

function help() {

	return "Allocate memory for a chunk on a superpeer's storage (temporary)\n" +
			"Parameters: id size\n" +
			"  id: Id to identify the chunk in the storage\n" +
			"  size: Size of the chunk to create";
}

function exec(id, size) {

    if (id == null) {
        dxterm.printlnErr("No id specified");
        return;
    }

    if (size == null) {
        dxterm.printlnErr("No size specified");
        return;
    }

    var tmpstore = dxram.service("tmpstore");

    if (tmpstore.create(id, size)) {
        dxterm.printfln("Created chunk of size %d in temporary storage: 0x%X", size, id);
    } else {
        dxterm.printlnErr("Creating chunk in temporary storage failed");
    }
}
