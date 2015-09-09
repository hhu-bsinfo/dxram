
package de.uniduesseldorf.dxram.commands;

/**
 * Clear console.
 * @author Michael Schoettner 03.09.2015
 */
public class CmdClear extends AbstractCmd {

	/**
	 * Constructor
	 */
	public CmdClear() {
	}

	@Override
	public String getName() {
		return "clear";
	}

	@Override
	public String getUsageMessage() {
		return "clear";
	}

	@Override
	public String getHelpMessage() {
		return "Clears the console.";
	}

	@Override
	public String getSyntax() {
		return "clear";
	}

	// called after parameter have been checked
	@Override
	public boolean execute(final String p_command) {
		// ANSI escape codes (clear screen, move cursor to first row and first column)
		System.out.print("\033[H\033[2J");
		System.out.flush();
		return true;
	}
}
