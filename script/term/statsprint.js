function imports() {

}

function help() {

	return  "Prints all statistics\n" +
	        "Parameters: [className]\n" +
	        "  className: Filter statistics by class name, optional";
}

function exec(className) {

    var stats = dxram.service("stats");

    if (className == null) {
        stats.printStatistics();
    } else {
        stats.printStatistics(className);
    }
}
