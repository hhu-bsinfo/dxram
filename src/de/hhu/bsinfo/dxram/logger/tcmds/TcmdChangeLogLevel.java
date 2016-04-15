
package de.hhu.bsinfo.dxram.logger.tcmds;

import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * This class handles the loggerlevel command which changes the logger level.
 * The available logger levels are: ISABLED, ERROR, WARN, INFO, DEBUG, TRACE
 * @author Mike
 */

public class TcmdChangeLogLevel extends AbstractTerminalCommand {

	private static final Argument MS_ARG_LEVEL =
			new Argument("level", null, false, "available LogLevels: DISABLED ERROR WARN INFO DEBUG TRACE");

	@Override
	public String getName() {

		return "loggerlevel";
	}

	@Override
	public String getDescription() {

		return "changes log level via terminal";
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
