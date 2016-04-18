
package de.hhu.bsinfo.dxram.logger.tcmds;

import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * This class handles the loggerlevel command which changes the logger level.
 * The available logger levels are: DISABLED, ERROR, WARN, INFO, DEBUG, TRACE
 * @author Michael Birkhoff <michael.birkhoff@hhu.de> 18.04.16
 */

public class TcmdChangeLogLevel extends AbstractTerminalCommand {

	private static final Argument MS_ARG_LEVEL =
			new Argument("level", null, false,
					"Log level to set, available levels: DISABLED ERROR WARN INFO DEBUG TRACE");

	@Override
	public String getName() {

		return "loggerlevel";
	}

	@Override
	public String getDescription() {

		return "Change the output level of the logger";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_LEVEL);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {

		String level = p_arguments.getArgumentValue(MS_ARG_LEVEL, String.class);

		if (level == null) {
			return false;
		}

		LoggerService logService = getTerminalDelegate().getDXRAMService(LoggerService.class);
		logService.setLogLevel(level);

		return true;
	}

}
