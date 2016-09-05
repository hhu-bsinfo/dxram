
package de.hhu.bsinfo.dxram.term.tcmds;

import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Terminal command to quit/shutdown the terminal.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
 */
public class TcmdQuit extends AbstractTerminalCommand {

	private boolean m_askOnQuit;

	/**
	 * Constructor
	 *
	 * @param p_askOnQuit True to ask "Are you sure" before quitting
	 */
	public TcmdQuit(final boolean p_askOnQuit) {
		m_askOnQuit = p_askOnQuit;
	}

	@Override
	public String getName() {
		return "quit";
	}

	@Override
	public String getDescription() {
		return "Quit console and shutdown node.";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		if (m_askOnQuit) {
			if (getTerminalDelegate().areYouSure()) {
				getTerminalDelegate().exitTerminal();
			}
		} else {
			getTerminalDelegate().exitTerminal();
		}

		return true;
	}

}
