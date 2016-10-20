function imports() {

}

function help() {

	return "List all available tasks that are registered and can be executed"
}

function exec(nid) {

    var mscomp = dxram.service("mscomp");
    var payloads = mscomp.getRegisteredTaskPayloads();

    dxterm.println("Registered task payload classes (" + payloads.size() + "): ");
    for each (payload in payloads) {
        dxterm.println(payload.getValue().getSimpleName() + ": " + (payload.getKey() >> 16 & 0xFFFF) + ", "
                       							+ (payload.getKey() & 0xFFFF));
    }
}
