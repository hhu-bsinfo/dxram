package de.hhu.bsinfo.dxram.term;

import java.util.List;

import de.hhu.bsinfo.dxram.script.ScriptEngineComponent;

/**
 * Created by nothaas on 10/14/16.
 */
public class ScriptTerminalContext {

	private ScriptEngineComponent m_scriptEngine;
	private List<String> m_commands;

	public ScriptTerminalContext(final ScriptEngineComponent p_scriptEngine, final List<String> p_commands) {
		m_scriptEngine = p_scriptEngine;
		m_commands = p_commands;
	}

	public void list() {
		String str = "";

		for (String item : m_commands) {
			str += item + ", ";
		}

		System.out.println(str.substring(0, str.length() - 2));
	}

	public Command cmd(final String p_name) {
		return new Command(m_scriptEngine, p_name);
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
}
