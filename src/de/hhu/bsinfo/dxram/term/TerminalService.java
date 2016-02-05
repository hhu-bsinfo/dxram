package de.hhu.bsinfo.dxram.term;

import java.util.Map.Entry;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMService;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.utils.JNIconsole;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentListParser;
import de.hhu.bsinfo.utils.args.DefaultArgumentListParser;

public class TerminalService extends DXRAMService implements TerminalDelegate
{
	private LoggerComponent m_logger = null;
	private BootComponent m_boot = null;
	private TerminalComponent m_terminal = null;
	
	private boolean m_loop = true;
	
	// only returns if terminal was exited
	public void loop()
	{
		String command;
		String[] arguments;
		byte[] arr;
		ArgumentListParser argsParser = new DefaultArgumentListParser();
		ArgumentList argsList = new ArgumentList();
		
		m_logger.info(getClass(), "Running terminal...");
		
		System.out.println("DXRAM terminal v. 0.1");
		System.out.println("Running on node " + m_boot.getNodeID() + ", role " + m_boot.getNodeRole());
		System.out.println("Enter '?' to list all available commands.");
		System.out.println("Use '? <command>' to get information about a command.");

		while (m_loop) {
			arr = JNIconsole.readline("$" + m_boot.getNodeID() + "> ");
			if (arr != null) {
				command = new String(arr, 0, arr.length);
				arguments = command.split(" ");
				
				if (arguments[0].equals("?")) {
					if (arguments.length > 1) {
						final TerminalCommand c = m_terminal.getRegisteredCommands().get(arguments[1]);
						if (c == null) {
							System.out.println("error: unknown command");
						} else {
							printHelpMessage(c);
						}
					} else {
						System.out.println(getAvailableCommands());
					}
				} else {
					final TerminalCommand c = m_terminal.getRegisteredCommands().get(arguments[0]);
					if (c == null) {
						System.out.println("error: unknown command");
					} else {
						argsList.clear();
						argsParser.parseArguments(arguments, argsList);
						c.setTerminalDelegate(this);
						if (!c.execute(argsList)) {
							printUsage(c);
						}
					}	
				}
			}
		}	
		
		m_logger.info(getClass(), "Exiting terminal...");
	}
	
	@Override
	protected void registerDefaultSettingsService(Settings p_settings) {

	}

	@Override
	protected boolean startService(de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			Settings p_settings) {
		m_logger = getComponent(LoggerComponent.class);
		m_boot = getComponent(BootComponent.class);
		m_terminal = getComponent(TerminalComponent.class);
		
		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_logger = null;
		m_boot = null;
		m_terminal = null;
		
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
	public boolean areYouSure() {
		boolean ret;
		byte[] arr;

		while (true) {
			System.out.print("Are you sure (y/n)?");

			arr = JNIconsole.readline("");
			if (arr != null) {
				if (arr[0] == 'y' || arr[0] == 'Y') {
					ret = true;
					break;
				} else if (arr[0] == 'n' || arr[0] == 'N') {
					ret = false;
					break;
				}
			} else {
				ret = false;
				break;
			}
		}

		return ret;
	}

	@Override
	public <T extends DXRAMService> T getDXRAMService(Class<T> p_class) {
		return getServiceAccessor().getService(p_class);
	}

	@Override
	public <T extends DXRAMComponent> T getDXRAMComponent(Class<T> p_class) {
		return getComponent(p_class);
	}
	
	private String getAvailableCommands() {
		String str = new String();
		for (Entry<String, TerminalCommand> entry : m_terminal.getRegisteredCommands().entrySet()) {
			str += entry.getValue().getName() + ", ";
		}
		
		return str;
	}
	
	private void printUsage(final TerminalCommand p_command) {
		System.out.println("  usage: " + p_command.getUsageMessage());
	}
	
	private void printHelpMessage(final TerminalCommand p_command) {
		String[] lines;

		System.out.println("  usage:       " + p_command.getUsageMessage());
		System.out.println();

		lines = p_command.getHelpMessage().split("\n");
		// we should never end up here
		if (lines == null) {
			return;
		}
		System.out.println("  description: " + lines[0]);
		for (int i = 1; i < lines.length; i++) {
			System.out.println("               " + lines[i]);
		}
	}
}
