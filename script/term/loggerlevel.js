function imports() {

}

function help() {

	return "Change the output level of the logger\n" +
			"Parameters: level [nid]\n" +
			"  level: Log level to set, available levels (str): disabled, error, warn, info, debug, trace\n" +
			"  nid: Change the log level of another node, defaults to current node";
}

function exec(level, nid) {

    if (level == null) {
        dxterm.printlnErr("No level specified");
        return;
    }

    var logger = dxram.service("logger");

    if (nid == null) {
        logger.setLogLevel(level);
    } else {
        logger.setLogLevel(level, nid);
    }
}
