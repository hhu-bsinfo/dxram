
package de.hhu.bsinfo.dxram.term;

import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Base class any command that can be executed in the terminal.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
 */
public abstract class AbstractTerminalCommand {
	private TerminalDelegate m_terminalDelegate;

	/**
	 * Get name of command.
	 * @return String with name.
	 */
	public abstract String getName();

	/**
	 * Get a description of the command.
	 * @return Description message.
	 */
	public abstract String getDescription();

	/**
	 * Register all arguments (required and optional) that are used by this command.
	 * Default arguments are also applied here if specified within the argument.
	 * @param p_arguments
	 *            Argument list to register all used arguments at.
	 */
	public abstract void registerArguments(final ArgumentList p_arguments);

	/**
	 * Execute the command. Put any logic that needs to be executed here.
	 * Make sure to check the values you are getting from the argument list
	 * if they are valid (non null) if you have not specified a default argument
	 * for them and the argument was marked as optional.
	 * @param p_arguments
	 *            Arguments passed to this command.
	 * @return True if execution was succesful, false otherwise.
	 */
	public abstract boolean execute(final ArgumentList p_arguments);

	// --------------------------------------------------------------------------------------------

	/**
	 * Set the terminal delegate to allow access to certain terminal features within the command.
	 * @param p_terminalDelegate
	 *            Terminal delegate to set.
	 */
	void setTerminalDelegate(final TerminalDelegate p_terminalDelegate) {
		m_terminalDelegate = p_terminalDelegate;
	}

	/**
	 * Get the terminal delegate to get access to certain features within the command.
	 * @return Terminal delegate.
	 */
	protected TerminalDelegate getTerminalDelegate() {
		return m_terminalDelegate;
	}
}
