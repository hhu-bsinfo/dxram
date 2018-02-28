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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Semaphore;

import de.hhu.bsinfo.dxram.ms.MasterNodeEntry;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.ms.TaskListener;
import de.hhu.bsinfo.dxram.ms.TaskScript;
import de.hhu.bsinfo.dxram.ms.TaskScriptNode;
import de.hhu.bsinfo.dxram.ms.TaskScriptState;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdin;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;

/**
 * Submit a task to a compute group
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdComptask extends AbstractTerminalCommand {
    public TcmdComptask() {
        super("comptask");
    }

    @Override
    public String getHelp() {
        return "Submit a task to a compute group\n" + "Usage: comptask <taskName> <cgid> [minSlaves] [maxSlaves] [wait] ...\n" +
                "  taskName: String of the fully qualified class name of the task\n" + "  cgid: Id of the compute group to submit the task to\n" +
                "  minSlaves: Minimum number of slaves required to start the task (default 0 = arbitrary)\n" +
                "  maxSlaves: Maximum number of slaves for this task (default 0 = arbitrary)\n" +
                "  wait: Wait/block until the task is completed (default true)\n" +
                "  ...: Task arguments as further parameters depending on the task (default none)";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServerStdin p_stdin,
            final TerminalServiceAccessor p_services) {
        String taskName = p_cmd.getArgString(0, null);
        short cgid = p_cmd.getArgShort(1, (short) -1);
        short minSlaves = p_cmd.getArgShort(2, (short) 0);
        short maxSlaves = p_cmd.getArgShort(3, (short) 0);
        boolean wait = p_cmd.getArgBoolean(4, true);

        if (taskName == null) {
            p_stdout.printlnErr("No task name specified");
            return;
        }

        if (cgid == -1) {
            p_stdout.printlnErr("No cgid specified");
            return;
        }

        MasterSlaveComputeService mscomp = p_services.getService(MasterSlaveComputeService.class);
        TaskScriptNode task;
        if (p_cmd.getArgc() >= 5) {
            task = MasterSlaveComputeService.createTaskInstance(taskName, Arrays.copyOfRange(p_cmd.getArgs(), 5, p_cmd.getArgc()));
        } else {
            task = MasterSlaveComputeService.createTaskInstance(taskName);
        }

        if (task == null) {
            p_stdout.printlnErr("Creating task failed");
            return;
        }

        TaskScript taskScript = new TaskScript(minSlaves, maxSlaves, "Terminal", task);

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
            p_stdout.printlnErr("Task submission failed");
            return;
        }

        p_stdout.printfln("Task %s submitted, payload id: %d", task, taskState.getTaskScriptIdAssigned());

        if (wait) {
            p_stdout.println("Waiting for task to finish...");

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
                mscomp = p_services.getService(MasterSlaveComputeService.class);
                MasterSlaveComputeService.StatusMaster status = mscomp.getStatusMaster(p_cmdStr.getArgShort(1, (short) 0));
                for (int i = 0; i <= status.getConnectedSlaves().size(); i++) {
                    list.add(Integer.toString(i));
                }

                break;

            case 3:
                mscomp = p_services.getService(MasterSlaveComputeService.class);
                status = mscomp.getStatusMaster(p_cmdStr.getArgShort(1, (short) 0));
                int min = p_cmdStr.getArgInt(2, 0);
                for (int i = 0; i <= status.getConnectedSlaves().size(); i++) {
                    if (i >= min) {
                        list.add(Integer.toString(i));
                    }
                }

                break;

            case 4:
                return TcmdUtils.getBooleanCompSuggestions();

            default:
                break;
        }

        return list;
    }
}
