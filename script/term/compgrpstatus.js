function imports() {

}

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
        dxterm.printflnErr("Getting compute group status of group %d failed", cgid);
        return;
    }

    dxterm.printfln("Status of group %d:\n%s", cgid, status);
}
