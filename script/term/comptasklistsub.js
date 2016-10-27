function imports() {

    importClass(Packages.java.util.concurrent.Semaphore);
    importPackage(Packages.de.hhu.bsinfo.dxcompute.ms);
}

function help() {

	return "Submit a list of tasks loaded from a file\n" +
	        "Parameters: fileName cgid wait\n" +
            "  fileName: Path to a task list file\n" +
            "  cgid: Id of the compute group to submit the tasks to\n" +
            "  wait: Wait/block until all tasks completed";
}

function exec(fileName, cgid, wait) {

    if (fileName == null) {
        dxterm.printlnErr("No fileName specified");
        return;
    }

    if (cgid == null) {
        dxterm.printlnErr("No cgid specified");
        return;
    }

    if (wait == null) {
        dxterm.printlnErr("No wait specified");
        return;
    }

    var mscomp = dxram.service("mscomp");

    var jsonStr = dxram.readFile(fileName);

    if (jsonStr == null) {
        dxterm.printflnErr("Reading file '%s' failed", pathTaskFile);
        return;
    }

    var payloadList = mscomp.readTaskPayloadListFromJson(jsonStr);

    if (payloadList == null) {
        dxterm.printflnErr("Reading payloadList from task file '%s' failed");
        return;
    }

    var sem = new Semaphore(-(payloadList.length - 1), false);

    for each (payload in payloadList) {

        var task = new Task(payload, "Term");

        if (task == null) {
            dxterm.printflnErr("Creating task for payload %s failed", payload);
            return;
        }

        task.registerTaskListener(new TaskListener({
            taskBeforeExecution: function(task) {
                dxterm.printfln("ComputeTask: Starting execution %s", task);
            },


            taskCompleted: function(task) {
                dxterm.printfln("ComputeTask: Finished execution %s", task);
                dxterm.println("Return codes of slave nodes:");
                var results = task.getExecutionReturnCodes();

                for (var i = 0; i < results.length; i++) {

                    if (results[i] != 0) {
                        dxterm.printflnErr("(%d%: %d", i, results[i]);
                    } else {
                        dxterm.printfln("(%d%: %d", i, results[i]);
                    }
                }

                sem.release();
            }
        }));

        var payloadId = mscomp.submitTask(task, cgid);

        if (payloadId == -1) {
            dxterm.printflnErr("Task submission of %s failed", task);
            return;
        }

        dxterm.printfln("Task %s submitted, payload id: %d", task, payloadId);
    }

    if (wait) {
        dxterm.println("Waiting for all tasks to finish...");

        try {
            sem.acquire();
        } catch (e) {

        }
    }
}
