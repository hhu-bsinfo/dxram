package de.hhu.bsinfo.dxram.term;

import java.util.Map.Entry;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.utils.JNIconsole;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentListParser;
import de.hhu.bsinfo.utils.args.DefaultArgumentListParser;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * Service providing an interactive terminal running on a DXRAM instance.
 * Allows access to implemented services, triggering commands, getting information
 * about current or remote DXRAM instances.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
 */
public class TerminalService extends AbstractDXRAMService implements TerminalDelegate
{
	private LoggerComponent m_logger = null;
	private AbstractBootComponent m_boot = null;
	private TerminalComponent m_terminal = null;
	
	private boolean m_loop = true;
	
	/**
	 * Run the terminal loop.
	 * Only returns if terminal was exited.
	 */
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
		System.out.println("Use '!' or '! <command>' for interactive mode.");

		while (m_loop) {
			arr = JNIconsole.readline("$0x" + Integer.toHexString(m_boot.getNodeID()).substring(4).toUpperCase() + "> ");
			if (arr != null) {
				command = new String(arr, 0, arr.length);
				arguments = command.split(" ");
				
				if (arguments[0].equals("?")) {
					if (arguments.length > 1) {
						final TerminalCommand c = m_terminal.getRegisteredCommands().get(arguments[1]);
						if (c == null) {
							System.out.println("error: unknown command");
						} else {
							printUsage(c);
						}
					} else {
						System.out.println("Available commands:");
						System.out.println(getAvailableCommands());
					}
				} else if (arguments[0].equals("!") || arguments[0].equals("!")) {
					String cmdStr = null;
					if (arguments.length < 2)
					{
						System.out.println("Specify command for interactive mode:");
						cmdStr = promptForUserInput("command");
					} else {
						cmdStr = arguments[1];
					}
					final TerminalCommand c = m_terminal.getRegisteredCommands().get(cmdStr);
					if (c == null) {
						System.out.println("error: unknown command");
					} else {
						argsList.clear();
						c.registerArguments(argsList);
						
						// trigger interactive mode
						System.out.println("Interactive argument input for '" + c.getName() + "':");
						if (!interactiveArgumentMode(argsList))
						{
							System.out.println("error entering arguments");
						}
						
						if (!argsList.checkArguments())
						{
							printUsage(c);
						}
						else
						{
							c.setTerminalDelegate(this);
							if (!c.execute(argsList)) {
								printUsage(c);
							}
						}
					}
				} else {
					final TerminalCommand c = m_terminal.getRegisteredCommands().get(arguments[0]);
					if (c == null) {
						System.out.println("error: unknown command");
					} else {
						argsList.clear();
						c.registerArguments(argsList);
						argsParser.parseArguments(arguments, argsList);

						if (!argsList.checkArguments())
						{
							printUsage(c);
						}
						else
						{
							c.setTerminalDelegate(this);
							if (!c.execute(argsList)) {
								printUsage(c);
							}
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
		m_boot = getComponent(AbstractBootComponent.class);
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
	public String promptForUserInput(final String p_header) 
	{
		byte[] arr = JNIconsole.readline(p_header + "> ");
		if (arr != null) 
		{
			if (arr.length == 0)
				return null;
			else
				return new String(arr, 0, arr.length);
		}
		else
			return null;
	}

	@Override
	public <T extends AbstractDXRAMService> T getDXRAMService(Class<T> p_class) {
		return getServiceAccessor().getService(p_class);
	}
	
	/**
	 * Get a list of available/registered commands.
	 * @return List of registered commands.
	 */
	private String getAvailableCommands() {
		String str = new String();
		for (Entry<String, TerminalCommand> entry : m_terminal.getRegisteredCommands().entrySet()) {
			str += entry.getValue().getName() + ", ";
		}
		
		return str;
	}
	
	/**
	 * Print a usage message for the specified terminal command.
	 * @param p_command Terminal command to print usage message of.
	 */
	private void printUsage(final TerminalCommand p_command) {
		ArgumentList argList = new ArgumentList();
		// create default argument list
		p_command.registerArguments(argList);

		System.out.println("Command '" + p_command.getName() + "':");
		System.out.println(p_command.getDescription());
		System.out.println(argList.createUsageDescription(p_command.getName()));
	}
	
	/**
	 * Execute interactive argument mode to allow the user entering arguments for a command one by one.
	 * @param p_arguments List of arguments with arguments that need values to be entered.
	 * @return If user entered arguments properly, false otherwise.
	 */
	private boolean interactiveArgumentMode(final ArgumentList p_arguments)
	{
		// ask for non optional entries first
		for (Entry<String, Argument> entry : p_arguments.getArgumentMap().entrySet())
		{
			Argument arg = entry.getValue();
			if (!arg.isOptional())
			{
				String input = promptForUserInput("<" + arg.getKey() + "> ");
				if (input == null)
					return false;
				p_arguments.setArgument(arg.getKey(), input, "");
			}
		}
		
		// now go for optional entries
		for (Entry<String, Argument> entry : p_arguments.getArgumentMap().entrySet())
		{
			Argument arg = entry.getValue();
			if (arg.isOptional())
			{
				String input = promptForUserInput("[" + arg.getKey() + "] ");
				if (input != null)
					p_arguments.setArgument(arg.getKey(), input, "");
			}
		}
		
		return true;
	}
}
