
package de.hhu.bsinfo.dxram.term;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;

/**
 * Delegate to allow terminal commands to access certain features of the
 * terminal for command execution.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
 */
public interface TerminalDelegate {
	/**
	 * Prompt a "Are you sure?" message on the terminal and ask for confirmation.
	 * @return True if the answer was yes, false if no.
	 */
	boolean areYouSure();

	/**
	 * Trigger an exit of the terminal.
	 */
	void exitTerminal();

	/**
	 * Prompt the terminal user for input.
	 * @param p_header
	 *            Header/Description for the prompt.
	 * @return Null for no input or string with the user inputed.
	 */
	String promptForUserInput(final String p_header);

	/**
	 * Get a service from the DXRAM engine.
	 * @param <T>
	 *            Class extending DXRAM service
	 * @param p_class
	 *            Class of the service to get.
	 * @return DXRAMService or null if service not available.
	 */
	<T extends AbstractDXRAMService> T getDXRAMService(final Class<T> p_class);

	/**
	 * Execute another terminal command within a terminal command.
	 * @param p_cmdString
	 *            String to pass to the terminal to execute.
	 * @return True if execution was successful, false otherwise.
	 */
	boolean executeTerminalCommand(final String p_cmdString);
}
