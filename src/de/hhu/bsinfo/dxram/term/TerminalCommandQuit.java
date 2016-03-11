
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
	public String getDescription() {
		return "Quit console and shutdown node.";
	}
	
	@Override
	public void registerArguments(ArgumentList p_arguments) {
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
