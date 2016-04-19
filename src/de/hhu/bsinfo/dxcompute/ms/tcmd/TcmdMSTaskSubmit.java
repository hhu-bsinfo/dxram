
package de.hhu.bsinfo.dxcompute.ms.tcmd;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxcompute.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxcompute.ms.Task;
import de.hhu.bsinfo.dxcompute.ms.TaskListener;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

public class TcmdMSTaskSubmit extends AbstractTerminalCommand implements TaskListener {
	private static final Argument MS_ARG_TASK_TYPE_ID =
			new Argument("tid", null, false, "Type id of the task to submit");
	private static final Argument MS_ARG_TASK_SUBTYPE_ID =
			new Argument("stid", null, false, "Subtype id of the task to submit");
	private static final Argument MS_ARG_NID =
			new Argument("nid", null, false, "Master node id to submit the task to");
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
		p_arguments.setArgument(MS_ARG_NID);
		p_arguments.setArgument(MS_ARG_NAME);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		Short tid = p_arguments.getArgumentValue(MS_ARG_TASK_TYPE_ID, Short.class);
		Short stid = p_arguments.getArgumentValue(MS_ARG_TASK_SUBTYPE_ID, Short.class);
		Short nid = p_arguments.getArgumentValue(MS_ARG_NID, Short.class);
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

		Task task = new Task(payload, name + ms_taskCounter);
		task.registerTaskListener(this);

		long taskId = computeService.submitTask(task, nid);
		if (taskId == -1) {
			System.out.println("Submitting task " + task + " to node " + NodeID.toHexString(nid) + " failed.");
			return true;
		}

		System.out.println("Task submitted to node " + NodeID.toHexString(nid) + ", task id " + taskId);

		return true;
	}

	@Override
	public void taskBeforeExecution(final Task p_task) {
		System.out.println("ComputeTask: Starting execution " + p_task);
	}

	@Override
	public void taskCompleted(final Task p_task) {
		System.out.println("ComputeTask: Finished execution " + p_task);
	}
}
