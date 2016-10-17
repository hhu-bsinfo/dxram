package de.hhu.bsinfo.dxram.term;

import de.hhu.bsinfo.dxram.script.ScriptEngineComponent;

/**
 * Created by nothaas on 10/14/16.
 */
public class ScriptTerminalContext {

	private ScriptEngineComponent m_scriptEngine;
	private TerminalComponent m_terminal;

	public ScriptTerminalContext(final ScriptEngineComponent p_scriptEngine, final TerminalComponent p_terminal) {
		m_scriptEngine = p_scriptEngine;
		m_terminal = p_terminal;
	}

	public void print(final String p_str) {
		System.out.print(p_str);
	}

	public void println(final String p_str) {
		System.out.println(p_str);
	}

	public void printErr(final String p_str) {
		changeConsoleColor(TerminalColor.RED, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
		System.out.print(p_str);
		changeConsoleColor(TerminalColor.DEFAULT, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
	}

	public void printlnErr(final String p_str) {
		changeConsoleColor(TerminalColor.RED, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
		System.out.println(p_str);
		changeConsoleColor(TerminalColor.DEFAULT, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
	}

	public void list() {
		String str = "";

		for (String item : m_terminal.getRegisteredCommands().keySet()) {
			str += item + ", ";
		}

		System.out.println(str.substring(0, str.length() - 2));
	}

	public void reload() {
		m_terminal.reloadTerminalScripts();
	}

	public Command cmd(final String p_name) {
		return new Command(m_scriptEngine, p_name);
	}

	public void help() {
		System.out.println("TODO write help");
	}

	public static class Command {

		private ScriptEngineComponent m_scriptEngine;
		private String m_name;

		public Command(final ScriptEngineComponent p_scriptEngine, final String p_name) {
			m_scriptEngine = p_scriptEngine;
			m_name = p_name;
		}

		public void help() {
			System.out.println(m_scriptEngine.getContext(m_name).call("help"));
		}

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
		} else if (p_backgroundColor == TerminalColor.DEFAULT && p_color != TerminalColor.DEFAULT) {
			System.out.printf("\033[%d;%dm", p_style.ordinal(), p_color.ordinal() + 30);
		} else {
			System.out.printf("\033[%dm", p_style.ordinal());
		}
	}
}
