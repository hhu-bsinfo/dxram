/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

	return "Submit a list of tasks loaded from a file\n" +
	        "Usage: comptaskscript(fileName, cgid, wait)\n" +
            "  fileName: Path to a task script file\n" +
            "  cgid: Id of the compute group to submit the task script to\n" +
            "  wait: Wait/block until script completed";
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
    var taskScript = mscomp.readTaskScriptFromJsonFile(fileName);

    if (taskScript == null) {
        dxterm.printflnErr("Reading task script from file '%s' failed", fileName);
        return;
    }

    var sem = new Semaphore(0, false);
    var listener = new TaskListener({
        taskBeforeExecution: function(taskScriptState) {
            dxterm.printfln("ComputeTask: Starting execution %s", taskScriptState);
        },


        taskCompleted: function(taskScriptState) {
            dxterm.printfln("ComputeTask: Finished execution %s", taskScriptState);
            dxterm.println("Return codes of slave nodes: ");
            var results = taskScriptState.getExecutionReturnCodes();

            for (var i = 0; i < results.length; i++) {
                if (results[i] != 0) {
                    dxterm.printflnErr("(%d): %d", dxram.toInt(i), results[i]);
                } else {
                    dxterm.printfln("(%d): %d", dxram.toInt(i), results[i]);
                }
            }

            sem.release();
        }
    });

    var taskState = mscomp.submitTaskScript(taskScript, cgid, listener);

    if (taskState == null) {
        dxterm.printlnErr("Task script submission failed");
        return;
    }

    dxterm.printfln("Task script %s submitted, payload id: %d", taskScript, taskState.getTaskScriptIdAssigned());

    if (wait) {
        dxterm.println("Waiting for task script to finish...");

        try {
            sem.acquire();
        } catch (e) {

        }
    }
}
