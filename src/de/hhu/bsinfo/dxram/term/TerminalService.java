
package de.hhu.bsinfo.dxram.term;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.JNIconsole;

/**
 * Service providing an interactive terminal running on a DXRAM instance.
 * Allows access to implemented services, triggering commands, getting information
 * about current or remote DXRAM instances.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
 */
public class TerminalService extends AbstractDXRAMService {
	private LoggerComponent m_logger;
	private AbstractBootComponent m_boot;
	private TerminalComponent m_terminal;

	private boolean m_loop = true;
	private BufferedWriter m_historyFile;

	private String m_autostartScript;

	/**
	 * Constructor
	 */
	public TerminalService() {
		super("term");
	}

	/**
	 * Run the terminal loop.
	 * Only returns if terminal was exited.
	 */
	public void loop() {
		byte[] arr;

		if (!m_boot.getNodeRole().equals(NodeRole.TERMINAL)) {
			System.out.println("A Terminal node must have the NodeRole \"terminal\". Aborting");
			return;
		}

		// register commands for autocompletion
		{
			// TODO have prefix "term.XXX"
			String[] commandNames = m_terminal.getRegisteredCommands().keySet().toArray(new String[0]);
			JNIconsole.autocompleteCommands(commandNames);
		}

		// #if LOGGER >= INFO
		m_logger.info(getClass(), "Running terminal...");
		// #endif /* LOGGER >= INFO */

		System.out.println(">>> DXRAM Terminal <<<");
		System.out.println(
				"Running on node " + NodeID.toHexString(m_boot.getNodeID()) + ", role " + m_boot.getNodeRole());

		// TODO autostart script file
		//		if (!m_autostartScript.isEmpty()) {
		//			System.out.println("Running auto start script " + m_autostartScript);
		//			executeTerminalCommand("dsh file:" + m_autostartScript);
		//		}

		while (m_loop) {
			arr = JNIconsole
					.readline("$" + NodeID.toHexString(m_boot.getNodeID()) + "> ");
			if (arr != null) {
				String command = new String(arr, 0, arr.length);

				try {
					if (m_historyFile != null) {
						m_historyFile.write(command + "\n");
					}
				} catch (IOException e) {
					// #if LOGGER >= ERROR
					m_logger.error(getClass(), "Writing history file failed", e);
					// #endif /* LOGGER >= ERROR */
				}

				execute(command);
			}
		}

		// #if LOGGER >= INFO
		m_logger.info(getClass(), "Exiting terminal...");
		// #endif /* LOGGER >= INFO */
	}

	private void execute(final String p_text) {

		// skip empty
		if (p_text.isEmpty()) {
			return;
		}

		if (p_text.startsWith("?")) {
			m_terminal.getScriptTerminalContext().help();
		} else if (p_text.equals("exit")) {
			m_loop = false;
		} else {
			eveluateCommand(p_text);
		}
	}

	private void eveluateCommand(final String p_text) {
		// resolve terminal cmd "macros"
		String[] tokensFunc = p_text.split("\\(");
		String[] tokensHelp = p_text.split(" ");

		// print help for cmd
		if (tokensHelp.length > 1 && tokensHelp[0].equals("help")) {
			de.hhu.bsinfo.dxram.script.ScriptContext scriptCtx = m_terminal.getRegisteredCommands().get(tokensHelp[1]);
			if (scriptCtx != null) {
				m_terminal.getScriptContext().eval("dxterm.cmd(\"" + tokensHelp[1] + "\").help()");
			} else {
				System.out.println("Could not find help for terminal command '" + tokensHelp[1] + "'");
			}
		} else if (tokensFunc.length > 1) {

			// resolve cmd call
			de.hhu.bsinfo.dxram.script.ScriptContext scriptCtx = m_terminal.getRegisteredCommands().get(tokensFunc[0]);
			if (scriptCtx != null) {
				// assemble long call
				String call = "dxterm.cmd(\"" + tokensFunc[0] + "\").exec(";

				// prepare parameters
				if (tokensFunc[1].length() > 1) {
					call += tokensFunc[1];
				} else {
					call += ")";
				}

				m_terminal.getScriptContext().eval(call);
			} else {
				m_terminal.getScriptContext().eval(p_text);
			}
		} else {
			// filter some generic "macros"
			if (p_text.equals("help")) {
				m_terminal.getScriptTerminalContext().help();
			} else {
				m_terminal.getScriptContext().eval(p_text);
			}
		}
	}

	@Override
	protected void registerDefaultSettingsService(final Settings p_settings) {
		p_settings.setDefaultValue(TerminalConfigurationValues.Service.AUTOSTART_SCRIPT);
	}

	@Override
	protected boolean startService(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {
		m_logger = getComponent(LoggerComponent.class);
		m_boot = getComponent(AbstractBootComponent.class);
		m_terminal = getComponent(TerminalComponent.class);

		if (m_boot.getNodeRole() == NodeRole.TERMINAL) {
			loadHistoryFromFile("dxram_term_history");
		}

		m_autostartScript = p_settings.getValue(TerminalConfigurationValues.Service.AUTOSTART_SCRIPT);

		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_logger = null;
		m_boot = null;
		m_terminal = null;

		if (m_historyFile != null) {
			try {
				m_historyFile.close();
			} catch (final IOException ignored) {
			}
		}

		return true;
	}

	//	@Override
	//	public void exitTerminal() {
	//		m_loop = false;
	//	}
	//
	//	@Override
	//	public boolean areYouSure() {
	//		boolean ret;
	//		byte[] arr;
	//
	//		while (true) {
	//			System.out.print("Are you sure (y/n)?");
	//
	//			arr = JNIconsole.readline("");
	//			if (arr != null && arr.length > 0) {
	//				if (arr[0] == 'y' || arr[0] == 'Y') {
	//					ret = true;
	//					break;
	//				} else if (arr[0] == 'n' || arr[0] == 'N') {
	//					ret = false;
	//					break;
	//				}
	//			} else {
	//				ret = false;
	//				break;
	//			}
	//		}
	//
	//		return ret;
	//	}
	//
	//	@Override
	//	public String promptForUserInput(final String p_header) {
	//		byte[] arr = JNIconsole.readline(p_header + "> ");
	//		if (arr != null) {
	//			if (arr.length == 0) {
	//				return null;
	//			} else {
	//				return new String(arr, 0, arr.length);
	//			}
	//		} else {
	//			return null;
	//		}
	//	}
	//
	//	@Override
	//	public void print(final String p_str) {
	//		System.out.print(p_str);
	//	}
	//
	//	@Override
	//	public void print(final Object p_object) {
	//		System.out.print(p_object);
	//	}
	//
	//	@Override
	//	public void print(final String p_str, final TerminalColor p_color) {
	//		changeConsoleColor(p_color, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
	//		System.out.print(p_str);
	//		changeConsoleColor(TerminalColor.DEFAULT, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
	//	}
	//
	//	@Override
	//	public void print(final String p_str, final TerminalColor p_color, final TerminalColor p_backgroundColor,
	//			final TerminalStyle p_style) {
	//		changeConsoleColor(p_color, p_backgroundColor, p_style);
	//		System.out.print(p_str);
	//		changeConsoleColor(TerminalColor.DEFAULT, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
	//	}
	//
	//	@Override
	//	public void println() {
	//		System.out.println();
	//	}
	//
	//	@Override
	//	public void println(final String p_str) {
	//		System.out.println(p_str);
	//	}
	//
	//	@Override
	//	public void println(final Object p_object) {
	//		System.out.println(p_object);
	//	}
	//
	//	@Override
	//	public void println(final String p_str, final TerminalColor p_color) {
	//		changeConsoleColor(p_color, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
	//		System.out.print(p_str);
	//		changeConsoleColor(TerminalColor.DEFAULT, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
	//		System.out.println();
	//	}
	//
	//	@Override
	//	public void println(final String p_str, final TerminalColor p_color, final TerminalColor p_backgroundColor,
	//			final TerminalStyle p_style) {
	//		changeConsoleColor(p_color, p_backgroundColor, p_style);
	//		System.out.print(p_str);
	//		changeConsoleColor(TerminalColor.DEFAULT, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
	//		System.out.println();
	//	}
	//
	//	@Override
	//	public void clear() {
	//		// ANSI escape codes (clear screen, move cursor to first row and first column)
	//		System.out.print("\033[H\033[2J");
	//		System.out.flush();
	//	}
	//
	//	@Override
	//	public <T extends AbstractDXRAMService> T getDXRAMService(final Class<T> p_class) {
	//		return getServiceAccessor().getService(p_class);
	//	}
	//
	//	@Override
	//	public boolean executeTerminalCommand(final String p_cmdString) {
	//		//			String[] arguments;
	//		//			ArgumentListParser argsParser = new DefaultArgumentListParser();
	//		//			ArgumentList argsList = new ArgumentList();
	//		//
	//		//			// ignore comments
	//		//			if (p_cmdString.trim().startsWith("#")) {
	//		//				return true;
	//		//			}
	//		//
	//		//			arguments = p_cmdString.split(" ");
	//		//
	//		//			if (arguments[0].equals("?")) {
	//		//				if (arguments.length > 1) {
	//		//					final AbstractTerminalCommand c = m_terminal.getRegisteredCommands().get(arguments[1]);
	//		//					if (c == null) {
	//		//						System.out.println("error: unknown command");
	//		//						return false;
	//		//					} else {
	//		//						printUsage(c);
	//		//					}
	//		//				} else {
	//		//					System.out.println("Available commands:");
	//		//					System.out.println(getAvailableCommands());
	//		//				}
	//		//			} else if (arguments[0].equals("!") || arguments[0].equals("!")) {
	//		//				String cmdStr;
	//		//				if (arguments.length < 2) {
	//		//					System.out.println("Specify command for interactive mode:");
	//		//					cmdStr = promptForUserInput("command");
	//		//				} else {
	//		//					cmdStr = arguments[1];
	//		//				}
	//		//				final AbstractTerminalCommand c = m_terminal.getRegisteredCommands().get(cmdStr);
	//		//				if (c == null) {
	//		//					System.out.println("error: unknown command");
	//		//					return false;
	//		//				} else {
	//		//					argsList.clear();
	//		//					c.registerArguments(argsList);
	//		//
	//		//					// trigger interactive mode
	//		//					System.out.println("Interactive argument input for '" + c.getName() + "':");
	//		//					if (!interactiveArgumentMode(argsList)) {
	//		//						System.out.println("error entering arguments");
	//		//					}
	//		//
	//		//					if (!argsList.checkArguments()) {
	//		//						printUsage(c);
	//		//						return false;
	//		//					} else {
	//		//						c.setTerminalDelegate(this);
	//		//						if (!c.execute(argsList)) {
	//		//							printUsage(c);
	//		//							return false;
	//		//						}
	//		//					}
	//		//				}
	//		//			} else if (arguments[0].equals("$")) {
	//		//				if (arguments[1].equals("load")) {
	//		//					if (!m_scriptEngine.loadScriptFile(arguments[2])) {
	//		//						System.out.println("error loading script file " + arguments[2]);
	//		//					}
	//		//				} else {
	//		//
	//		//				}
	//		//			} else {
	//		//				if (arguments[0].isEmpty()) {
	//		//					return true;
	//		//				}
	//		//
	//		//				final AbstractTerminalCommand c = m_terminal.getRegisteredCommands().get(arguments[0]);
	//		//				if (c == null) {
	//		//					System.out.println("error: unknown command");
	//		//					return false;
	//		//				} else {
	//		//					argsList.clear();
	//		//					c.registerArguments(argsList);
	//		//					try {
	//		//						argsParser.parseArguments(arguments, argsList);
	//		//					} catch (final Exception e) {
	//		//						System.out.println("error: parsing arguments. most likely invalid syntax");
	//		//						return false;
	//		//					}
	//		//
	//		//					if (!argsList.checkArguments()) {
	//		//						printUsage(c);
	//		//						return false;
	//		//					} else {
	//		//						c.setTerminalDelegate(this);
	//		//						if (!c.execute(argsList)) {
	//		//							printUsage(c);
	//		//							return false;
	//		//						}
	//		//					}
	//		//				}
	//		//			}
	//
	//		return true;
	//	}

	//	/**
	//	 * Get a list of available/registered commands.
	//	 *
	//	 * @return List of registered commands.
	//	 */
	//	private String getAvailableCommands() {
	//		String str = new String();
	//		Collection<String> commands = m_terminal.getRegisteredCommands().keySet();
	//		List<String> sortedList = new ArrayList<String>(commands);
	//		Collections.sort(sortedList);
	//		boolean first = true;
	//		for (String cmd : sortedList) {
	//			if (first) {
	//				first = false;
	//			} else {
	//				str += ", ";
	//			}
	//			str += cmd;
	//		}
	//
	//		return str;
	//	}

	//	/**
	//	 * Print a usage message for the specified terminal command.
	//	 *
	//	 * @param p_command Terminal command to print usage message of.
	//	 */
	//	private void printUsage(final AbstractTerminalCommand p_command) {
	//		ArgumentList argList = new ArgumentList();
	//		// create default argument list
	//		p_command.registerArguments(argList);
	//
	//		System.out.println("Command '" + p_command.getName() + "':");
	//		System.out.println(p_command.getDescription());
	//		System.out.println(argList.createUsageDescription(p_command.getName()));
	//	}
	//
	//	/**
	//	 * Execute interactive argument mode to allow the user entering arguments for a command one by one.
	//	 *
	//	 * @param p_arguments List of arguments with arguments that need values to be entered.
	//	 * @return If user entered arguments properly, false otherwise.
	//	 */
	//	private boolean interactiveArgumentMode(final ArgumentList p_arguments) {
	//		// ask for non optional entries first
	//		for (Entry<String, Argument> entry : p_arguments.getArgumentMap().entrySet()) {
	//			Argument arg = entry.getValue();
	//			if (!arg.isOptional()) {
	//				String input = promptForUserInput("<" + arg.getKey() + "> ");
	//				if (input == null) {
	//					return false;
	//				}
	//				p_arguments.setArgument(arg.getKey(), input, "");
	//			}
	//		}
	//
	//		// now go for optional entries
	//		for (Entry<String, Argument> entry : p_arguments.getArgumentMap().entrySet()) {
	//			Argument arg = entry.getValue();
	//			if (arg.isOptional()) {
	//				String input = promptForUserInput("[" + arg.getKey() + "] ");
	//				if (input != null) {
	//					p_arguments.setArgument(arg.getKey(), input, "");
	//				}
	//			}
	//		}
	//
	//		return true;
	//	}

	/**
	 * Change the color of stdout.
	 *
	 * @param p_color           Text color.
	 * @param p_backgroundColor Shell background color
	 * @param p_style           Text style.
	 */
	private void changeConsoleColor(final TerminalColor p_color, final TerminalColor p_backgroundColor,
			final TerminalStyle p_style) {
		if (p_backgroundColor != TerminalColor.DEFAULT) {
			System.out.printf("\033[%d;%d;%dm", p_style.ordinal(), p_color.ordinal() + 30,
					p_backgroundColor.ordinal() + 40);
		} else if (p_backgroundColor == TerminalColor.DEFAULT && p_color != TerminalColor.DEFAULT) {
			System.out.printf("\033[%d;%dm", p_style.ordinal(), p_color.ordinal() + 30);
		} else {
			System.out.printf("\033[%dm", p_style.ordinal());
		}
	}

	/**
	 * Load terminal command history from a file.
	 *
	 * @param p_file File to load the history from and append new commands to.
	 */
	private void loadHistoryFromFile(final String p_file) {
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(p_file));

			// import history if found
			String str;
			while (true) {
				try {
					str = reader.readLine();
				} catch (IOException e) {
					break;
				}

				if (str == null) {
					break;
				}

				JNIconsole.addToHistory(str);
			}

			reader.close();
		} catch (final FileNotFoundException e) {
			// #if LOGGER >= DEBUG
			m_logger.debug(getClass(), "No history found: " + p_file);
			// #endif /* LOGGER >= DEBUG */
		} catch (final IOException e) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Reading history " + p_file + " failed", e);
			// #endif /* LOGGER >= ERROR */
		}

		try {
			m_historyFile = new BufferedWriter(new FileWriter(p_file, true));
		} catch (final IOException e) {
			m_historyFile = null;
			// #if LOGGER >= WARN
			m_logger.warn(getClass(), "Opening history " + p_file + " for writing failed", e);
			// #endif /* LOGGER >= WARN */
		}
	}
}
