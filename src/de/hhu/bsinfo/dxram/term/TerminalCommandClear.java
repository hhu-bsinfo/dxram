
package de.hhu.bsinfo.dxram.term;

import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Clear console.
 * @author Michael Schoettner 03.09.2015
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
