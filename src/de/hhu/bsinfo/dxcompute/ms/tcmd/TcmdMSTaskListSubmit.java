
package de.hhu.bsinfo.dxcompute.ms.tcmd;

import java.io.File;

import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.conf.Configuration;
import de.hhu.bsinfo.utils.conf.ConfigurationException;
import de.hhu.bsinfo.utils.conf.ConfigurationParser;
import de.hhu.bsinfo.utils.conf.ConfigurationXMLLoader;
import de.hhu.bsinfo.utils.conf.ConfigurationXMLLoaderFile;
import de.hhu.bsinfo.utils.conf.ConfigurationXMLParser;

public class TcmdMSTaskListSubmit extends AbstractTerminalCommand {

	private static final Argument MS_ARG_CGID =
			new Argument("cgid", null, false, "Id of the compute group to submit the tasks to");
	private static final Argument MS_ARG_NAME =
			new Argument("file", null, false, "Task list file");

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
		p_arguments.setArgument(MS_ARG_NAME);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		short cgid = p_arguments.getArgument(MS_ARG_CGID).getValue(Short.class);
		String file = p_arguments.getArgument(MS_ARG_NAME).getValue(String.class);

		Configuration taskList = loadTaskList(file);
		if (taskList != null) {

		}

		return true;
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

	private ArrayList<> parseTaskList(final Configuration p_taskList) {

	}

	private void submitTaskList(final ArrayList<>)
}
