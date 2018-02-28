/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxterm.cmd;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import de.hhu.bsinfo.dxram.ms.MasterNodeEntry;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.ms.TaskListener;
import de.hhu.bsinfo.dxram.ms.TaskScript;
import de.hhu.bsinfo.dxram.ms.TaskScriptState;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdin;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;

/**
 * Submit a list of tasks loaded from a file
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdComptaskscript extends AbstractTerminalCommand {
    public TcmdComptaskscript() {
        super("comptaskscript");
    }

    @Override
    public String getHelp() {
        return "Submit a list of tasks loaded from a file\n" + "Usage: comptaskscript <fileName> <cgid> [wait]\n" + "  fileName: Path to a task script file\n" +
                "  cgid: Id of the compute group to submit the task script to\n" + "  wait: Wait/block until script completed (default: true)";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServerStdin p_stdin,
            final TerminalServiceAccessor p_services) {
        String fileName = p_cmd.getArgString(0, null);
        short cgid = p_cmd.getArgShort(1, (short) -1);
        boolean wait = p_cmd.getArgBoolean(2, true);

        if (fileName == null) {
            p_stdout.printlnErr("No fileName specified");
            return;
        }

        if (cgid == -1) {
            p_stdout.printlnErr("No cgid specified");
            return;
        }

        MasterSlaveComputeService mscomp = p_services.getService(MasterSlaveComputeService.class);
        TaskScript taskScript = MasterSlaveComputeService.readTaskScriptFromJsonFile(fileName);

        if (taskScript == null) {
            p_stdout.printflnErr("Reading task script from file '%s' failed", fileName);
            return;
        }

        Semaphore sem = new Semaphore(0, false);
        TaskListener listener = new TaskListener() {

            @Override
            public void taskBeforeExecution(final TaskScriptState p_taskScriptState) {
                p_stdout.printfln("ComputeTask: Starting execution %s", p_taskScriptState);
            }

            @Override
            public void taskCompleted(final TaskScriptState p_taskScriptState) {
                p_stdout.printfln("ComputeTask: Finished execution %s", p_taskScriptState);
                p_stdout.println("Return codes of slave nodes: ");
                int[] results = p_taskScriptState.getExecutionReturnCodes();

                for (int i = 0; i < results.length; i++) {
                    if (results[i] != 0) {
                        p_stdout.printflnErr("(%d): %d", i, results[i]);
                    } else {
                        p_stdout.printfln("(%d): %d", i, results[i]);
                    }
                }

                sem.release();
            }
        };

        TaskScriptState taskState = mscomp.submitTaskScript(taskScript, cgid, listener);

        if (taskState == null) {
            p_stdout.printlnErr("Task script submission failed");
            return;
        }

        p_stdout.printfln("Task script %s submitted, payload id: %d", taskScript, taskState.getTaskScriptIdAssigned());

        if (wait) {
            p_stdout.println("Waiting for task script to finish...");

            try {
                sem.acquire();
            } catch (final InterruptedException ignored) {

            }
        }
    }

    @Override
    public List<String> getArgumentCompletionSuggestions(final int p_argumentPos, final TerminalCommandString p_cmdStr,
            final TerminalServiceAccessor p_services) {
        List<String> list = new ArrayList<String>();

        switch (p_argumentPos) {
            case 1:
                MasterSlaveComputeService mscomp = p_services.getService(MasterSlaveComputeService.class);
                ArrayList<MasterNodeEntry> masters = mscomp.getMasters();

                for (MasterNodeEntry entry : masters) {
                    list.add(Short.toString(entry.getComputeGroupId()));
                }

                break;

            case 2:
                return TcmdUtils.getBooleanCompSuggestions();

            default:
                break;
        }

        return list;
    }
}
