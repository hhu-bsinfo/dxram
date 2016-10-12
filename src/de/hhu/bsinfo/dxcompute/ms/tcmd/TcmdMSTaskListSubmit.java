
package de.hhu.bsinfo.dxcompute.ms.tcmd;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

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
import de.hhu.bsinfo.utils.conf.Configuration;
import de.hhu.bsinfo.utils.conf.ConfigurationException;
import de.hhu.bsinfo.utils.conf.ConfigurationParser;
import de.hhu.bsinfo.utils.conf.ConfigurationXMLLoader;
import de.hhu.bsinfo.utils.conf.ConfigurationXMLLoaderFile;
import de.hhu.bsinfo.utils.conf.ConfigurationXMLParser;

/**
 * Terminal command to read a list of tasks from file, create task payloads and pass them to a compute group for
 * execution.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 27.04.16
 */
public class TcmdMSTaskListSubmit extends AbstractTerminalCommand implements TaskListener {

	private static final Argument MS_ARG_CGID =
			new Argument("cgid", null, false, "Id of the compute group to submit the tasks to");
	private static final Argument MS_ARG_FILE =
			new Argument("file", null, false, "Task list file to parse");
	private static final Argument MS_ARG_NAME =
			new Argument("name", "TcmdTask", true, "Name for the tasks for easier identification");
	private static final Argument MS_ARG_WAIT =
			new Argument("wait", "false", true, "Wait/block until all tasks submitted are completed");

	private static int m_taskCounter;
	private Semaphore m_finished;

	@Override
	public String getName() {
		return "comptasklistsubmit";
	}

	@Override
	public String getDescription() {
		return "Submit a list of tasks loaded from a file.";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_CGID);
		p_arguments.setArgument(MS_ARG_FILE);
		p_arguments.setArgument(MS_ARG_NAME);
		p_arguments.setArgument(MS_ARG_WAIT);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		short cgid = p_arguments.getArgument(MS_ARG_CGID).getValue(Short.class);
		String file = p_arguments.getArgument(MS_ARG_FILE).getValue(String.class);
		String name = p_arguments.getArgument(MS_ARG_NAME).getValue(String.class);
		boolean wait = p_arguments.getArgumentValue(MS_ARG_WAIT, Boolean.class);

		MasterSlaveComputeService computeService =
				getTerminalDelegate().getDXRAMService(MasterSlaveComputeService.class);

		Configuration taskList = loadTaskList(file);
		if (taskList == null) {
			return true;
		}

		ArrayList<AbstractTaskPayload> taskPayloads = parseTaskList(taskList);
		m_finished = new Semaphore(-(taskPayloads.size() - 1), false);
		for (AbstractTaskPayload taskPayload : taskPayloads) {
			Task task = new Task(taskPayload, name + m_taskCounter++);
			task.registerTaskListener(this);

			long taskId = computeService.submitTask(task, cgid);
			if (taskId == -1) {
				getTerminalDelegate().println("ERR: Submitting task " + task + " to compute group " + cgid + " failed.",
						TerminalColor.RED);
				return true;
			}
		}

		getTerminalDelegate().println("Submitted " + taskPayloads.size() + " to cgid " + cgid);

		if (wait) {
			getTerminalDelegate().println("Waiting for all tasks to finish...");
			try {
				m_finished.acquire();
			} catch (final InterruptedException ignored) {

			}
		}

		return true;
	}

	@Override
	public void taskBeforeExecution(final Task p_task) {
		getTerminalDelegate().println("ComputeTask: Starting execution " + p_task);
	}

	@Override
	public void taskCompleted(final Task p_task) {
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

		m_finished.release();
	}

	/**
	 * Load a list of tasks from a file (.ctask).
	 *
	 * @param p_file Task list file
	 * @return Configuration object if successful, null otherwise.
	 */
	private Configuration loadTaskList(final String p_file) {
		Configuration taskList = new Configuration("TaskList");
		ConfigurationXMLLoader loader = new ConfigurationXMLLoaderFile(p_file);
		ConfigurationParser parser = new ConfigurationXMLParser(loader);

		try {
			parser.readConfiguration(taskList);
		} catch (final ConfigurationException e) {
			// check if file exists -> save default config later
			if (new File(p_file).exists()) {
				getTerminalDelegate().println("Parsing task list " + p_file + " failed.", TerminalColor.RED);
				return null;
			} else {
				getTerminalDelegate().println("Could not parse task list " + p_file + " file not found.",
						TerminalColor.RED);
				return null;
			}
		}

		return taskList;
	}

	/**
	 * Parse the configuration file containing the task list.
	 *
	 * @param p_taskList Task list to parse.
	 * @return List of task payload objects created from the provided list.
	 */
	private ArrayList<AbstractTaskPayload> parseTaskList(final Configuration p_taskList) {
		ArrayList<AbstractTaskPayload> taskList = new ArrayList<>();

		Map<Integer, Short> tids = p_taskList.getValues("/ComputeTask/tid", Short.class);
		Map<Integer, Short> stids = p_taskList.getValues("/ComputeTask/stid", Short.class);

		// make sure the entries are sorted by the index
		ArrayList<Entry<Integer, Short>> entryList = new ArrayList<>(tids.size());
		// i haven't found a better way...
		entryList.addAll(tids.entrySet());

		// sort
		Collections.sort(entryList, (p_entry1, p_entry2) -> {
			int e1 = p_entry1.getKey();
			int e2 = p_entry2.getKey();

			if (e1 < e2) {
				return -1;
			} else if (e1 > e2) {
				return 1;
			} else {
				return 0;
			}
		});

		for (Entry<Integer, Short> tidEntry : entryList) {
			Short tid = tidEntry.getValue();
			Short stid = stids.get(tidEntry.getKey());

			if (stid == null) {
				// ignore missing stid
				continue;
			}

			AbstractTaskPayload taskPayload;
			try {
				taskPayload = TaskPayloadManager.createInstance(tid, stid);
			} catch (final RuntimeException e) {
				getTerminalDelegate().println(
						"Cannot create task payload with tid " + tid + ", stid " + stid + ", not registered.",
						TerminalColor.RED);
				continue;
			}

			// grab the arguments the task expects
			ArgumentList taskPayloadArguments = new ArgumentList();
			ArgumentList taskPayloadArgumentsConfig = new ArgumentList();
			taskPayload.terminalCommandRegisterArguments(taskPayloadArguments);

			// grab the values from the config
			for (Argument argument : taskPayloadArguments.getArgumentMap().values()) {
				String value =
						p_taskList.getValue("/ComputeTask/" + argument.getKey(), tidEntry.getKey());
				if (value == null) {
					getTerminalDelegate()
							.println("Missing value for task id " + tidEntry.getValue() + " tid " + tid + ", stid "
									+ stid + " for key " + argument.getKey(), TerminalColor.RED);
					continue;
				}

				Argument argTmp = new Argument(argument.getKey(), value);
				taskPayloadArgumentsConfig.setArgument(argTmp);
			}

			try {
				taskPayload.terminalCommandCallbackForArguments(taskPayloadArgumentsConfig);
			} catch (final NullPointerException e) {
				// happens if an argument was not provided (probably typo)
				getTerminalDelegate()
						.println("Parsing arguments of task with type id " + tid + " subtype id " + stid + " for key "
								+ tidEntry.getKey() + " failed, missing or mistyped argument?", TerminalColor.RED);
				continue;
			}
			taskList.add(taskPayload);
		}

		return taskList;
	}
}
