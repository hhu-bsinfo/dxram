function imports() {

    importClass(Packages.java.util.concurrent.Semaphore);
    importPackage(Packages.de.hhu.bsinfo.dxcompute.ms);
}

function help() {

	return "Submit a task to a compute group\n" +
	        "Parameters (1): typeId subtypeId cgid wait ...\n" +
	        "Parameters (2): pathTaskFile cgid wait\n" +
	        "  typeId: Type id of the task to submit\n" +
	        "  subtypeId: Subtype id of the task to submit\n" +
            "  pathTaskFile: Path to a task file\n" +
            "  cgid: Id of the compute group to submit the task to\n" +
            "  wait: Wait/block until the task is completed\n" +
            "  ...: Task arguments as further parameters depending on the task";
}

function exec(arg1) {

    if (typeof arg1 === "string") {
        exec_taskFile.apply(this, arguments);
    } else {
        exec_taskId.apply(this, arguments);
    }
}

function exec_taskId(typeId, subtypeId, cgid, wait) {

    if (typeId == null) {
        dxterm.printlnErr("No typeId specified");
        return;
    }

    if (subtypeId == null) {
        dxterm.printlnErr("No subtypeId specified");
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
    var payload = mscomp.createTaskPayload(typeId, subtypeId, Array.prototype.slice.call(arguments, 4));

    if (payload == null) {
        dxterm.printlnErr("Creating task payload failed");
        return;
    }

    var task = mscomp.createTask(payload, "Term");

    if (task == null) {
        dxterm.printlnErr("Creating task failed");
        return;
    }

    var sem = new Semaphore(0, false);
    task.registerTaskListener(new TaskListener({
        taskBeforeExecution: function(task) {
            dxterm.println("ComputeTask: Starting execution " + task);
        },


        taskCompleted: function(task) {
            dxterm.println("ComputeTask: Finished execution " + task);
            dxterm.println("Return codes of slave nodes: ");
            var results = task.getExecutionReturnCodes();

            for (var i = 0; i < results.length; i++) {

                if (results[i] != 0) {
                    dxterm.printErr("(" + i + "): " + results[i]);
                } else {
                    dxterm.println("(" + i + "): " + results[i]);
                }
            }

            sem.release();
        }
    }));

    var payloadId = mscomp.submitTask(task, cgid);

    if (payloadId == -1) {
        dxterm.printlnErr("Task submission failed");
        return;
    }

    dxterm.println("Task " + task + " submitted, payload id: " + payloadId);

    if (wait) {
        dxterm.println("Waiting for task to finish...");

        try {
            sem.acquire();
        } catch (e) {

        }
    }
}

function exec_taskFile(pathTaskFile, cgid, wait) {

    if (pathTaskFile == null) {
        dxterm.printlnErr("No pathTaskFile specified");
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

    var jsonStr = dxram.readFile(pathTaskFile);

    if (jsonStr == null) {
        dxterm.printlnErr("Reading file '" + pathTaskFile + "' failed");
        return;
    }

    var payload = mscomp.readTaskPayloadFromJson(jsonStr);

    if (payload == null) {
        dxterm.printlnErr("Reading payload from task file '" + pathTaskFile + "' failed");
        return;
    }

    var task = mscomp.createTask(payload, "Term");

    if (task == null) {
        dxterm.printlnErr("Creating task failed");
        return;
    }

    var sem = new Semaphore(0, false);
    task.registerTaskListener(new TaskListener({
        taskBeforeExecution: function(task) {
            dxterm.println("ComputeTask: Starting execution " + task);
        },


        taskCompleted: function(task) {
            dxterm.println("ComputeTask: Finished execution " + task);
            dxterm.println("Return codes of slave nodes: ");
            var results = task.getExecutionReturnCodes();

            for (var i = 0; i < results.length; i++) {

                if (results[i] != 0) {
                    dxterm.printErr("(" + i + "): " + results[i]);
                } else {
                    dxterm.println("(" + i + "): " + results[i]);
                }
            }

            sem.release();
        }
    }));

    var payloadId = mscomp.submitTask(task, cgid);

    if (payloadId == -1) {
        dxterm.printlnErr("Task submission failed");
        return;
    }

    dxterm.println("Task " + task + " submitted, payload id: " + payloadId);

    if (wait) {
        dxterm.println("Waiting for task to finish...");

        try {
            sem.acquire();
        } catch (e) {

        }
    }
}
