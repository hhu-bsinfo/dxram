
package de.hhu.bsinfo.dxcompute.ms.tcmd;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxcompute.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxcompute.ms.Task;
import de.hhu.bsinfo.dxcompute.ms.TaskListener;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * Terminal command to submit a task to a compute group.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class TcmdMSTaskSubmit extends AbstractTerminalCommand implements TaskListener {
	private static final Argument MS_ARG_TASK_TYPE_ID =
			new Argument("tid", null, false, "Type id of the task to submit");
	private static final Argument MS_ARG_TASK_SUBTYPE_ID =
			new Argument("stid", null, false, "Subtype id of the task to submit");
	private static final Argument MS_ARG_CGID =
			new Argument("cgid", null, false, "Id of the compute group to submit the task to");
	private static final Argument MS_ARG_NAME =
			new Argument("name", "TcmdTask", true, "Name for the task for easier identification");

	private static int ms_taskCounter;

	@Override
	public String getName() {
		return "comptasksubmit";
	}

	@Override
	public String getDescription() {
		return "Submit a task to a compute group";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_TASK_TYPE_ID);
		p_arguments.setArgument(MS_ARG_TASK_SUBTYPE_ID);
		p_arguments.setArgument(MS_ARG_CGID);
		p_arguments.setArgument(MS_ARG_NAME);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		Short tid = p_arguments.getArgumentValue(MS_ARG_TASK_TYPE_ID, Short.class);
		Short stid = p_arguments.getArgumentValue(MS_ARG_TASK_SUBTYPE_ID, Short.class);
		Short cgid = p_arguments.getArgumentValue(MS_ARG_CGID, Short.class);
		String name = p_arguments.getArgumentValue(MS_ARG_NAME, String.class);

		MasterSlaveComputeService computeService =
				getTerminalDelegate().getDXRAMService(MasterSlaveComputeService.class);

		AbstractTaskPayload payload;
		try {
			payload = AbstractTaskPayload.createInstance(tid, stid);
		} catch (final Exception e) {
			System.out
					.println("Cannot create task with type id " + tid + " subtype id " + stid + ": " + e.getMessage());
			return true;
		}

		// prompt for additional parameters for the payload
		if (!payload.terminalCommandCallbackForParameters(getTerminalDelegate())) {
			return false;
		}

		Task task = new Task(payload, name + ms_taskCounter++);
		task.registerTaskListener(this);

		long taskId = computeService.submitTask(task, cgid);
		if (taskId == -1) {
			System.out.println("Submitting task " + task + " to compute group " + cgid + " failed.");
			return true;
		}

		System.out.println("Task submitted to compute group " + cgid + ", task id " + taskId);

		return true;
	}

	@Override
	public void taskBeforeExecution(final Task p_task) {
		System.out.println("ComputeTask: Starting execution " + p_task);
	}

	@Override
	public void taskCompleted(final Task p_task) {
		System.out.println("ComputeTask: Finished execution " + p_task);
		System.out.println("Return codes of slave nodes: ");
		int[] results = p_task.getExecutionReturnCodes();
		short[] slaves = p_task.getSlaveNodeIdsExecutingTask();
		for (int i = 0; i < results.length; i++) {
			System.out.println("(" + i + ") " + NodeID.toHexString(slaves[i]) + ": " + results[i]);
		}
	}
}
