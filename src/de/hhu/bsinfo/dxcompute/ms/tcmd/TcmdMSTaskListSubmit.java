
package de.hhu.bsinfo.dxcompute.ms.tcmd;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxcompute.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxcompute.ms.Task;
import de.hhu.bsinfo.dxcompute.ms.TaskListener;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.conf.Configuration;
import de.hhu.bsinfo.utils.conf.ConfigurationException;
import de.hhu.bsinfo.utils.conf.ConfigurationParser;
import de.hhu.bsinfo.utils.conf.ConfigurationXMLLoader;
import de.hhu.bsinfo.utils.conf.ConfigurationXMLLoaderFile;
import de.hhu.bsinfo.utils.conf.ConfigurationXMLParser;

public class TcmdMSTaskListSubmit extends AbstractTerminalCommand implements TaskListener {

	private static final Argument MS_ARG_CGID =
			new Argument("cgid", null, false, "Id of the compute group to submit the tasks to");
	private static final Argument MS_ARG_FILE =
			new Argument("file", null, false, "Task list file to parse");
	private static final Argument MS_ARG_NAME =
			new Argument("name", "TcmdTask", true, "Name for the tasks for easier identification");

	private static int ms_taskCounter;

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
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		short cgid = p_arguments.getArgument(MS_ARG_CGID).getValue(Short.class);
		String file = p_arguments.getArgument(MS_ARG_FILE).getValue(String.class);
		String name = p_arguments.getArgument(MS_ARG_NAME).getValue(String.class);

		MasterSlaveComputeService computeService =
				getTerminalDelegate().getDXRAMService(MasterSlaveComputeService.class);

		Configuration taskList = loadTaskList(file);
		if (taskList == null) {
			return true;
		}

		ArrayList<AbstractTaskPayload> taskPayloads = parseTaskList(taskList);
		for (AbstractTaskPayload taskPayload : taskPayloads) {
			Task task = new Task(taskPayload, name + ms_taskCounter++);
			task.registerTaskListener(this);

			long taskId = computeService.submitTask(task, cgid);
			if (taskId == -1) {
				System.out.println("ERR: Submitting task " + task + " to compute group " + cgid + " failed.");
				return true;
			}
		}

		System.out.println("Submitted " + taskPayloads.size() + " to cgid " + cgid);

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

	private Configuration loadTaskList(final String p_file) {
		Configuration taskList = new Configuration("TaskList");
		ConfigurationXMLLoader loader = new ConfigurationXMLLoaderFile(p_file);
		ConfigurationParser parser = new ConfigurationXMLParser(loader);

		try {
			parser.readConfiguration(taskList);
		} catch (final ConfigurationException e) {
			// check if file exists -> save default config later
			if (new File(p_file).exists()) {
				System.out.println("Parsing task list " + p_file + " failed.");
				return null;
			} else {
				System.out.println("Could not parse task list " + p_file + " file not found.");
				return null;
			}
		}

		return taskList;
	}

	private ArrayList<AbstractTaskPayload> parseTaskList(final Configuration p_taskList) {
		ArrayList<AbstractTaskPayload> taskList = new ArrayList<AbstractTaskPayload>();

		Map<Integer, Short> tids = p_taskList.getValues("/ComputeTask/tid", Short.class);
		Map<Integer, Short> stids = p_taskList.getValues("/ComputeTask/stid", Short.class);

		// make sure the entries are sorted by the index
		ArrayList<Entry<Integer, Short>> entryList = new ArrayList<Entry<Integer, Short>>(tids.size());
		// i haven't found a better way...
		for (Entry<Integer, Short> tidEntry : tids.entrySet()) {
			entryList.add(tidEntry);
		}

		// sort
		Collections.sort(entryList, new Comparator<Entry<Integer, Short>>() {
			@Override
			public int compare(final Entry<Integer, Short> p_entry1, final Entry<Integer, Short> p_entry2) {
				int e1 = p_entry1.getKey().intValue();
				int e2 = p_entry2.getKey().intValue();

				if (e1 < e2) {
					return -1;
				} else if (e1 > e2) {
					return 1;
				} else {
					return 0;
				}
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
				taskPayload = AbstractTaskPayload.createInstance(tid, stid);
			} catch (final RuntimeException e) {
				System.out
						.println("Cannot create task payload with tid " + tid + ", stid " + stid + ", not registered.");
				continue;
			}

			// grab the arguments the task expects
			ArgumentList taskPayloadArguments = new ArgumentList();
			ArgumentList taskPayloadArgumentsConfig = new ArgumentList();
			taskPayload.terminalCommandRegisterArguments(taskPayloadArguments);

			// grab the values from the config
			for (Argument argument : taskPayloadArguments.getArgumentMap().values()) {
				String value =
						p_taskList.getValue("/ComputeTask/" + argument.getKey(), tidEntry.getKey(), String.class);
				if (value == null) {
					System.out.println("Missing value for task id " + tidEntry.getValue() + " tid " + tid + ", stid "
							+ stid + " for key " + argument.getKey());
					continue;
				}

				Argument argTmp = new Argument(argument.getKey(), value);
				taskPayloadArgumentsConfig.setArgument(argTmp);
			}

			try {
				taskPayload.terminalCommandCallbackForArguments(taskPayloadArgumentsConfig);
			} catch (final NullPointerException e) {
				// happens if an argument was not provided (probably typo)
				System.out.println("Parsing arguments of task with type id " + tid + " subtype id " + stid + " for key "
						+ tidEntry.getKey() + " failed, missing or mistyped argument?");
			}
			taskList.add(taskPayload);
		}

		return taskList;
	}
}
