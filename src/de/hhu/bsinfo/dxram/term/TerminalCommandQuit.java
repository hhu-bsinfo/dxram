
package de.hhu.bsinfo.dxram.term;

import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Quit monitor.
 * @author Michael Schoettner 03.09.2015
 */
public class TerminalCommandQuit extends TerminalCommand {

	/**
	 * Constructor
	 */
	public TerminalCommandQuit() {}

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

	// called after parameter have been checked
	@Override
	public boolean execute(final ArgumentList p_arguments) {
		if (getTerminalDelegate().areYouSure()) {
			getTerminalDelegate().exitTerminal();
		} 

		return true;
	}

}
