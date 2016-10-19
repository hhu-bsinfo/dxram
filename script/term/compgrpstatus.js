function help() {

	return "Get the current status of a compute group\n" +
	        "Parameters: cgid\n" +
            "  cgid: Compute group id to get the status from";
}

function exec(cgid) {

    if (cgid == null) {
        dxterm.printlnErr("No cgid specified");
        return;
    }

    var mscomp = dxram.service("mscomp");
    var status = mscomp.getStatusMaster(cgid);

    if (status == null) {
        dxterm.printlnErr("Getting compute group status of group " + cgid + " failed");
        return;
    }

    dxterm.println("Status of group " + cgid + ":");
    dxterm.println(status);
}
