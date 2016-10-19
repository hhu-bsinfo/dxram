package de.hhu.bsinfo.dxram.term;

import de.hhu.bsinfo.dxram.script.ScriptEngineComponent;

/**
 * Context to be bound/exposed to the java script engine for the terminal
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 14.10.16
 */
public class ScriptTerminalContext {

	private ScriptEngineComponent m_scriptEngine;
	private TerminalComponent m_terminal;

	/**
	 * Constructor
	 *
	 * @param p_scriptEngine Script engine to be exposed to
	 * @param p_terminal     Parent terminal component reference
	 */
	public ScriptTerminalContext(final ScriptEngineComponent p_scriptEngine, final TerminalComponent p_terminal) {
		m_scriptEngine = p_scriptEngine;
		m_terminal = p_terminal;
	}

	/**
	 * Print to the console
	 *
	 * @param p_str String to print
	 */
	public void print(final String p_str) {
		System.out.print(p_str);
	}

	/**
	 * Print to a console + newline
	 *
	 * @param p_str String to print
	 */
	public void println(final String p_str) {
		System.out.println(p_str);
	}

	/**
	 * Print an error message to the console
	 *
	 * @param p_str String to print
	 */
	public void printErr(final String p_str) {
		changeConsoleColor(TerminalColor.RED, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
		System.out.print(p_str);
		changeConsoleColor(TerminalColor.DEFAULT, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
	}

	/**
	 * Print an error message to the conssole + newline
	 *
	 * @param p_str String to print
	 */
	public void printlnErr(final String p_str) {
		changeConsoleColor(TerminalColor.RED, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
		System.out.println(p_str);
		changeConsoleColor(TerminalColor.DEFAULT, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
	}

	/**
	 * List all available built in terminal commands/scripts
	 */
	public void list() {
		String str = "";

		for (String item : m_terminal.getRegisteredCommands().keySet()) {
			str += item + ", ";
		}

		System.out.println(str.substring(0, str.length() - 2));
	}

	/**
	 * Reload terminal commands/scripts
	 */
	public void reload() {
		m_terminal.reloadTerminalScripts();
	}

	/**
	 * Get a terminal command
	 *
	 * @param p_name Name of the command
	 * @return Command object
	 */
	public Command cmd(final String p_name) {
		return new Command(m_scriptEngine, p_name);
	}

	/**
	 * Print the terminal help message
	 */
	public void help() {
		System.out.println("Type '?' or 'help' to print this message\n"
				+ "> The terminal uses a java script engine, i.e. you can type java script code and execute it\n"
				+ "> Two contexts are available in the terminal: 'dxram' and 'dxterm' (refer to the classes for "
				+ "more information)\n"
				+ "> Built in terminal commands can be executed without the terminal context, i.e. nodelist() "
				+ "instead of dxterm.cmd(\"nodelist\").exec()\n"
				+ "> To list all built in terminal commands: dxterm.list()\n"
				+ "> Get help about a terminal command, example: 'help nodelist' or "
				+ "dxterm.cmd(\"nodelist\").help()");
	}

	/**
	 * Terminal command object (wrapper)
	 */
	public static class Command {

		private ScriptEngineComponent m_scriptEngine;
		private String m_name;

		/**
		 * Constructor
		 *
		 * @param p_scriptEngine Reference to the script engine to run on
		 * @param p_name         Name of the terminal command
		 */
		public Command(final ScriptEngineComponent p_scriptEngine, final String p_name) {
			m_scriptEngine = p_scriptEngine;
			m_name = p_name;
		}

		/**
		 * Load imports for the terminal command
		 */
		public void imports() {
			m_scriptEngine.getContext(m_name).call("imports");
		}

		/**
		 * Print the terminal command's help message
		 */
		public void help() {
			System.out.println(m_scriptEngine.getContext(m_name).call("help"));
		}

		/**
		 * Call the terminal command
		 *
		 * @param args Arguments for the command to call
		 * @return Return value of the command
		 */
		public Object exec(Object... args) {
			return m_scriptEngine.getContext(m_name).call("exec", args);
		}
	}

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
		} else if (p_color != TerminalColor.DEFAULT) {
			System.out.printf("\033[%d;%dm", p_style.ordinal(), p_color.ordinal() + 30);
		} else {
			System.out.printf("\033[%dm", p_style.ordinal());
		}
	}
}
