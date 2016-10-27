function imports() {

}

function help() {

	return "Get the status of the temporary (superpeer) storage";
}

function exec() {

    var tmpstore = dxram.service("tmpstore");
    var status = tmpstore.getStatus();

    if (status != null) {
        dxterm.printfln("Total size occupied (bytes): %d\n%s", status.calculateTotalDataUsageBytes(), status);
    } else {
        dxterm().printlnErr("Getting status of temporary storage failed");
    }
}
