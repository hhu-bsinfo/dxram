
package de.hhu.bsinfo.dxram.term;

import java.util.HashMap;
import java.util.Map;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.term.tcmds.TcmdClear;
import de.hhu.bsinfo.dxram.term.tcmds.TcmdPrint;
import de.hhu.bsinfo.dxram.term.tcmds.TcmdQuit;
import de.hhu.bsinfo.dxram.term.tcmds.TcmdScriptExec;
import de.hhu.bsinfo.dxram.term.tcmds.TcmdSleep;

/**
 * Component providing data/commands for an interactive terminal to be run on a node.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
 */
public class TerminalComponent extends AbstractDXRAMComponent {

	private LoggerComponent m_logger;

	private Map<String, AbstractTerminalCommand> m_commandMap = new HashMap<String, AbstractTerminalCommand>();

	/**
	 * Constructor
	 *
	 * @param p_priorityInit     Priority for initialization of this component.
	 *                           When choosing the order, consider component dependencies here.
	 * @param p_priorityShutdown Priority for shutting down this component.
	 *                           When choosing the order, consider component dependencies here.
	 */
	public TerminalComponent(final int p_priorityInit, final int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);

	}

	/**
	 * Register a new terminal command for the terminal.
	 *
	 * @param p_command Command to register.
	 * @return True if registering was successful, false if a command with the same name already exists.
	 */
	public boolean registerCommand(final AbstractTerminalCommand p_command) {
		// #if LOGGER >= DEBUG
		m_logger.debug(getClass(), "Registering command: " + p_command.getName());
		// #endif /* LOGGER >= DEBUG */

		return m_commandMap.putIfAbsent(p_command.getName(), p_command) == null;
	}

	/**
	 * Get all registered commands. This is used by the service, only.
	 *
	 * @return Map of registered commands.
	 */
	Map<String, AbstractTerminalCommand> getRegisteredCommands() {
		return m_commandMap;
	}

	@Override
	protected void registerDefaultSettingsComponent(final Settings p_settings) {
		p_settings.setDefaultValue(TerminalConfigurationValues.Component.ASK_ON_QUIT);
	}

	@Override
	protected boolean initComponent(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {
		m_logger = getDependentComponent(LoggerComponent.class);

		boolean askOnQuit = p_settings.getValue(TerminalConfigurationValues.Component.ASK_ON_QUIT);

		// register built in commands
		registerCommand(new TcmdClear());
		registerCommand(new TcmdQuit(askOnQuit));
		registerCommand(new TcmdPrint());
		registerCommand(new TcmdScriptExec());
		registerCommand(new TcmdSleep());

		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		m_logger = null;

		m_commandMap.clear();

		return true;
	}

}
