
package de.hhu.bsinfo.dxram.logger.tcmds;

import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.log.LogLevel;

/**
 * This class handles the loggerlevel command which changes the logger level.
 * The available logger levels are: DISABLED, ERROR, WARN, INFO, DEBUG, TRACE
 * @author Michael Birkhoff <michael.birkhoff@hhu.de> 18.04.16
 */

public class TcmdChangeLogLevel extends AbstractTerminalCommand {

	private static final Argument MS_ARG_LEVEL =
			new Argument("level", null, false,
					"Log level to set, available levels: DISABLED ERROR WARN INFO DEBUG TRACE");
	private static final Argument MS_ARG_NID =
			new Argument("nid", null, true, "Change the log level of another node");

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
		p_arguments.setArgument(MS_ARG_NID);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {

		String level = p_arguments.getArgumentValue(MS_ARG_LEVEL, String.class);
		Short nid = p_arguments.getArgumentValue(MS_ARG_NID, Short.class);

		LoggerService logService = getTerminalDelegate().getDXRAMService(LoggerService.class);

		LogLevel logLevel = LogLevel.toLogLevel(level);

		if (nid == null) {
			logService.setLogLevel(logLevel);
		} else {
			logService.setLogLevel(logLevel, nid);
		}

		return true;
	}

}
