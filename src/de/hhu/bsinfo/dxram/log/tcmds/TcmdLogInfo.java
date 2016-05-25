
package de.hhu.bsinfo.dxram.log.tcmds;

import de.hhu.bsinfo.dxram.log.LogService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * The class handles the loginfo command which prints the log of a specified node
 * @author Mike
 */

public class TcmdLogInfo extends AbstractTerminalCommand {

	private static final Argument MS_ARG_NID = new Argument("nid", null, false, "Node ID");

	@Override
	public String getName() {
		return "loginfo";
	}

	@Override
	public String getDescription() {
		return "Prints the log utilization of given peer";

	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_NID);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		Short nid = p_arguments.getArgumentValue(MS_ARG_NID, Short.class);

		LogService logService = getTerminalDelegate().getDXRAMService(LogService.class);

		getTerminalDelegate().println(logService.getCurrentUtilization(nid));

		return true;
	}

}
