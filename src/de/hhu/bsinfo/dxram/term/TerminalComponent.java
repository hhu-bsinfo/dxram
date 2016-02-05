package de.hhu.bsinfo.dxram.term;

import java.util.HashMap;
import java.util.Map;

import de.hhu.bsinfo.dxram.engine.DXRAMComponent;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;

public class TerminalComponent extends DXRAMComponent {

	private LoggerComponent m_logger = null;
	
	private Map<String, TerminalCommand> m_commandMap = new HashMap<String, TerminalCommand>();
	
	public TerminalComponent(int p_priorityInit, int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);

	}
	
	public boolean registerCommand(final TerminalCommand p_command)
	{
		m_logger.debug(getClass(), "Registering command: " + p_command.getName());
		return m_commandMap.putIfAbsent(p_command.getName(), p_command) == null;
	}
	
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
		
		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		m_logger = null;
		
		m_commandMap.clear();
		
		return true;
	}

}
