function help() {

	return "Get the status of the temporary (superpeer) storage";
}

function exec() {

    var tmpstore = dxram.service("tmpstore");
    var status = tmpstore.getStatus();

    if (status != null) {
        dxterm.println("Total size occupied (bytes): " + status.calculateTotalDataUsageBytes());
        dxterm.println(status);
    } else {
        dxterm().printlnErr("Getting status of temporary storage failed.");
    }
}
