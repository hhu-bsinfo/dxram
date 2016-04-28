
package de.hhu.bsinfo.dxram.term;

import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Terminal command to clear the console.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
 */
public class TerminalCommandClear extends AbstractTerminalCommand {

	/**
	 * Constructor
	 */
	public TerminalCommandClear() {}

	@Override
	public String getName() {
		return "clear";
	}

	@Override
	public String getDescription() {
		return "Clears the console.";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		getTerminalDelegate().clear();
		return true;
	}
}
