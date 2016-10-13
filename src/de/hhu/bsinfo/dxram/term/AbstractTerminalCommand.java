
package de.hhu.bsinfo.dxram.term;

/**
 * Base class any command that can be executed in the terminal.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
 */
public abstract class AbstractTerminalCommand {
	private TerminalDelegate m_terminalDelegate;

	/**
	 * Get name of command.
	 *
	 * @return String with name.
	 */
	public abstract String getName();

	/**
	 * Get a description of the command.
	 *
	 * @return Description message.
	 */
	public abstract String getDescription();

	// --------------------------------------------------------------------------------------------

	/**
	 * Set the terminal delegate to allow access to certain terminal features within the command.
	 *
	 * @param p_terminalDelegate Terminal delegate to set.
	 */
	void setTerminalDelegate(final TerminalDelegate p_terminalDelegate) {
		m_terminalDelegate = p_terminalDelegate;
	}

	/**
	 * Get the terminal delegate to get access to certain features within the command.
	 *
	 * @return Terminal delegate.
	 */
	protected TerminalDelegate getTerminalDelegate() {
		return m_terminalDelegate;
	}
}
