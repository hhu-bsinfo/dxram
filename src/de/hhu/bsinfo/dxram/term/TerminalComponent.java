package de.hhu.bsinfo.dxram.term;

import java.util.HashMap;
import java.util.Map;

import de.hhu.bsinfo.dxram.chunk.tcmds.TcmdEchoMessage;
import de.hhu.bsinfo.dxram.engine.DXRAMComponent;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;

/**
 * Component providing data/commands for an interactive terminal to be run on a node.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
 */
public class TerminalComponent extends DXRAMComponent {

	private LoggerComponent m_logger = null;
	
	private Map<String, TerminalCommand> m_commandMap = new HashMap<String, TerminalCommand>();
	
	/**
	 * Constructor
	 * @param p_priorityInit Priority for initialization of this component. 
	 * 			When choosing the order, consider component dependencies here.
	 * @param p_priorityShutdown Priority for shutting down this component. 
	 * 			When choosing the order, consider component dependencies here.
	 */
	public TerminalComponent(int p_priorityInit, int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);

	}
	
	/**
	 * Register a new terminal command for the terminal.
	 * @param p_command Command to register.
	 * @return True if registering was successful, false if a command with the same name already exists.
	 */
	public boolean registerCommand(final TerminalCommand p_command)
	{
		m_logger.debug(getClass(), "Registering command: " + p_command.getName());
		return m_commandMap.putIfAbsent(p_command.getName(), p_command) == null;
	}
	
	/**
	 * Get all registered commands. This is used by the service, only.
	 * @return Map of registered commands.
	 */
	Map<String, TerminalCommand> getRegisteredCommands()
	{
		return m_commandMap;
	}

	@Override
	protected void registerDefaultSettingsComponent(Settings p_settings) {

	}

	@Override
	protected boolean initComponent(de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			Settings p_settings) {
		m_logger = getDependentComponent(LoggerComponent.class);
		
		// register built in commands
		registerCommand(new TerminalCommandClear());
		registerCommand(new TerminalCommandQuit());
		registerCommand(new TcmdEchoMessage());
		
		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		m_logger = null;
		
		m_commandMap.clear();
		
		return true;
	}

}
