package de.hhu.bsinfo.dxram.term;

import java.util.HashMap;
import java.util.Map;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMService;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.utils.JNIconsole;

public class TerminalService extends DXRAMService implements TerminalDelegate
{
	private LoggerComponent m_logger = null;
	private BootComponent m_boot = null;
	
	private Map<String, TerminalCommand> m_commandMap = new HashMap<String, TerminalCommand>();
	private boolean m_loop = true;
	
	public boolean registerCommand(final TerminalCommand p_command)
	{
		m_logger.debug(getClass(), "Registering command: " + p_command.getName());
		p_command.setTerminalDelegate(this);
		return m_commandMap.putIfAbsent(p_command.getName(), p_command) == null;
	}
	
	// only returns if terminal was exited
	public void loop()
	{
		String command;
		String[] arguments;
		byte[] arr;

		System.out.println("DXRAM terminal v. 0.1");
		System.out.println("Running on node " + m_boot.getNodeID() + ", role " + m_boot.getNodeRole());
		System.out.println("Enter '?' to list all available commands.");

		while (m_loop) {
			arr = JNIconsole.readline();
			if (arr != null) {
				command = new String(arr, 0, arr.length);
				arguments = command.split(" ");

				final TerminalCommand c = m_commandMap.get(arguments[0]);
				if (c == null) {
					System.out.println("error: unknown command");
				} else {
					if (c.areParametersSane(arguments)) {
						c.execute(command);
					} else {
						c.printUsage();
					}
				}
			}
		}	
	}
	
	@Override
	protected void registerDefaultSettingsService(Settings p_settings) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected boolean startService(de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			Settings p_settings) {
		m_logger = getComponent(LoggerComponent.class);
		m_boot = getComponent(BootComponent.class);
	
		// register built in commands
		registerCommand(new TerminalCommandClear());
		registerCommand(new TerminalCommandQuit());
		
		// TODO read from config which commands to register?
		
	
		
		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_logger = null;
		m_boot = null;
		
		m_commandMap.clear();
		
		return true;
	}

	@Override
	protected boolean isServiceAccessor()
	{
		return true;
	}

	@Override
	public void exitTerminal() {
		m_loop = false;
	}

	@Override
	public <T extends DXRAMService> T getService(Class<T> p_class) {
		return getServiceAccessor().getService(p_class);
	}

	@Override
	public NodeRole nodeExists(short p_nodeID) {
		return m_boot.getNodeRole(p_nodeID);
	}
}
