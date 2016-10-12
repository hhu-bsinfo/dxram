
package de.hhu.bsinfo.dxcompute.ms.tcmd;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxcompute.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxcompute.ms.Task;
import de.hhu.bsinfo.dxcompute.ms.TaskListener;
import de.hhu.bsinfo.dxcompute.ms.TaskPayloadManager;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalColor;
import de.hhu.bsinfo.dxram.term.TerminalStyle;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * Terminal command to submit a task to a compute group.
 *
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
	private static final Argument MS_ARG_WAIT =
			new Argument("wait", "false", true, "Wait/block until the task is completed");

	private static int m_taskCounter;
	private Lock m_lock = new ReentrantLock(false);
	private Condition m_finished = m_lock.newCondition();

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
		p_arguments.setArgument(MS_ARG_WAIT);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		Short tid = p_arguments.getArgumentValue(MS_ARG_TASK_TYPE_ID, Short.class);
		Short stid = p_arguments.getArgumentValue(MS_ARG_TASK_SUBTYPE_ID, Short.class);
		Short cgid = p_arguments.getArgumentValue(MS_ARG_CGID, Short.class);
		String name = p_arguments.getArgumentValue(MS_ARG_NAME, String.class);
		boolean wait = p_arguments.getArgumentValue(MS_ARG_WAIT, Boolean.class);

		MasterSlaveComputeService computeService =
				getTerminalDelegate().getDXRAMService(MasterSlaveComputeService.class);

		AbstractTaskPayload payload;
		try {
			payload = TaskPayloadManager.createInstance(tid, stid);
		} catch (final Exception e) {
			getTerminalDelegate().println(
					"Cannot create task with type id " + tid + " subtype id " + stid + ": " + e.getMessage(),
					TerminalColor.RED);
			return true;
		}

		// get parameters for payload
		ArgumentList payloadRegisteredArguments = new ArgumentList();
		ArgumentList payloadCallbackArguments = new ArgumentList();
		payload.terminalCommandRegisterArguments(payloadRegisteredArguments);

		// check if parameters were already added via terminal command args
		for (Argument argument : payloadRegisteredArguments.getArgumentMap().values()) {
			Argument terminalCommandArg = p_arguments.getArgument(argument.getKey());
			if (terminalCommandArg == null) {
				// prompt user to type in argument
				terminalCommandArg =
						new Argument(argument.getKey(), getTerminalDelegate().promptForUserInput(argument.getKey()));
			}

			payloadCallbackArguments.setArgument(terminalCommandArg);
		}

		// provide arguments to task payload
		try {
			payload.terminalCommandCallbackForArguments(payloadCallbackArguments);
		} catch (final NullPointerException e) {
			// happens if an argument was not provided (probably typo)
			getTerminalDelegate().println("Parsing arguments of task with type id " + tid + " subtype id " + stid
					+ " failed, missing argument?");
		}
		Task task = new Task(payload, name + m_taskCounter++);
		task.registerTaskListener(this);

		long taskId = computeService.submitTask(task, cgid);
		if (taskId == -1) {
			getTerminalDelegate().println("Submitting task " + task + " to compute group " + cgid + " failed.");
			return true;
		}

		getTerminalDelegate().println("Task submitted to compute group " + cgid + ", task id " + taskId);

		m_lock.lock();
		if (wait) {
			m_lock.lock();
			getTerminalDelegate().println("Waiting for task to finish...");
			try {
				m_finished.await();
			} catch (final InterruptedException ignored) {

			}
		}
		m_lock.unlock();

		return true;
	}

	@Override
	public void taskBeforeExecution(final Task p_task) {
		getTerminalDelegate().println("ComputeTask: Starting execution " + p_task);
	}

	@Override
	public void taskCompleted(final Task p_task) {
		m_lock.lock();

		getTerminalDelegate().println("ComputeTask: Finished execution " + p_task);
		getTerminalDelegate().println("Return codes of slave nodes: ");
		int[] results = p_task.getExecutionReturnCodes();
		for (int i = 0; i < results.length; i++) {
			if (results[i] != 0) {
				getTerminalDelegate().println("(" + i + "): " + results[i],
						TerminalColor.YELLOW, TerminalColor.RED, TerminalStyle.NORMAL);
			} else {
				getTerminalDelegate().println("(" + i + "): " + results[i]);
			}
		}

		m_finished.signal();
		m_lock.unlock();
	}
}
