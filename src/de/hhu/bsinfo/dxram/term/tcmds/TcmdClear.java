
package de.hhu.bsinfo.dxram.term.tcmds;

import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;

/**
 * Terminal command to clear the console.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
 */
public class TcmdClear extends AbstractTerminalCommand {

	/**
	 * Constructor
	 */
	public TcmdClear() {
	}

	@Override
	public String getName() {
		return "clear";
	}

	@Override
	public String getDescription() {
		return "Clears the console.";
	}

	public void execute() {
		getTerminalDelegate().clear();
	}
}
