/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

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

    var task = new Task(payload, "Term");

    if (task == null) {
        dxterm.printlnErr("Creating task failed");
        return;
    }

    var sem = new Semaphore(0, false);
    task.registerTaskListener(new TaskListener({
        taskBeforeExecution: function(task) {
            dxterm.printfln("ComputeTask: Starting execution %s", task);
        },


        taskCompleted: function(task) {
            dxterm.printfln("ComputeTask: Finished execution %s", task);
            dxterm.println("Return codes of slave nodes: ");
            var results = task.getExecutionReturnCodes();

            for (var i = 0; i < results.length; i++) {

                if (results[i] != 0) {
                    dxterm.printflnErr("(%d): %d", i, results[i]);
                } else {
                    dxterm.printfln("(%d): %d", i, results[i]);
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

    dxterm.printfln("Task %s submitted, payload id: %d", task, payloadId);

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
        dxterm.printlnErr("Reading file '%s' failed", pathTaskFile);
        return;
    }

    var payload = mscomp.readTaskPayloadFromJson(jsonStr);

    if (payload == null) {
        dxterm.printflnErr("Reading payload from task file '%s' failed", pathTaskFile);
        return;
    }

    var task = new Task(payload, "Term");

    if (task == null) {
        dxterm.printlnErr("Creating task failed");
        return;
    }

    var sem = new Semaphore(0, false);
    task.registerTaskListener(new TaskListener({
        taskBeforeExecution: function(task) {
            dxterm.printfln("ComputeTask: Starting execution %s", task);
        },


        taskCompleted: function(task) {
            dxterm.printfln("ComputeTask: Finished execution %s", task);
            dxterm.println("Return codes of slave nodes: ");
            var results = task.getExecutionReturnCodes();

            for (var i = 0; i < results.length; i++) {

                if (results[i] != 0) {
                    dxterm.printflnErr("(%d): %d", i, results[i]);
                } else {
                    dxterm.printfln("(%d): %d", i, results[i]);
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

    dxterm.printfln("Task %s submitted, payload id: %d", task, payloadId);

    if (wait) {
        dxterm.println("Waiting for task to finish...");

        try {
            sem.acquire();
        } catch (e) {

        }
    }
}
