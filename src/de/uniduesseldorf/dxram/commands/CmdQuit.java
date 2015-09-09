
package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.Core;

/**
 * Quit monitor.
 * @author Michael Schoettner 03.09.2015
 */
public class CmdQuit extends AbstractCmd {

	/**
	 * Constructor
	 */
	public CmdQuit() {}

	@Override
	public String getName() {
		return "quit";
	}

	@Override
	public String getUsageMessage() {
		return "quit";
	}

	@Override
	public String getHelpMessage() {
		return "Quit console and shutdown node.";
	}

	@Override
	public String getSyntax() {
		return "quit";
	}

	// called after parameter have been checked
	@Override
	public boolean execute(final String p_command) {
		boolean ret = true;

		if (!areYouSure()) {
			ret = false;
		} else {
			Core.close();
			System.exit(0);
		}

		return ret;
	}

}
