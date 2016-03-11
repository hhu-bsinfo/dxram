
package de.hhu.bsinfo.dxram.term;

import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Terminal command to clear the console.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
 */
public class TerminalCommandClear extends TerminalCommand {

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
	public void registerArguments(ArgumentList p_arguments) {
	}

	// called after parameter have been checked
	@Override
	public boolean execute(final ArgumentList p_arguments) {
		// ANSI escape codes (clear screen, move cursor to first row and first column)
		System.out.print("\033[H\033[2J");
		System.out.flush();
		return true;
	}	
}
