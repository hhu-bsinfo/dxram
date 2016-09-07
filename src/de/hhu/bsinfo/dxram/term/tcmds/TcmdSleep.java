package de.hhu.bsinfo.dxram.term.tcmds;

import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Put the terminal to sleep for a specified amount of seconds.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
 */
public class TcmdSleep extends AbstractTerminalCommand {

	private static final ArgumentList.Argument
			MS_ARG_SEC = new ArgumentList.Argument("sec", null, false, "Time in seconds");

	/**
	 * Constructor
	 */
	public TcmdSleep() {
	}

	@Override
	public String getName() {
		return "sleep";
	}

	@Override
	public String getDescription() {
		return "Sleep for the specified amount of seconds.";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_SEC);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		int sec = p_arguments.getArgumentValue(MS_ARG_SEC, Integer.class);
		try {
			Thread.sleep(sec * 1000);
		} catch (final InterruptedException ignored) {
			return false;
		}
		return true;
	}
}
