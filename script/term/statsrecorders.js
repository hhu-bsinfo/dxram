function imports() {

}

function help() {

	return "Prints all available statistics recorders"
}

function exec() {

    var recorders = dxram.service("stats").getRecorders();

    for each (recorder in recorders) {
        dxterm.println("> " + recorder.getName());
    }
}
