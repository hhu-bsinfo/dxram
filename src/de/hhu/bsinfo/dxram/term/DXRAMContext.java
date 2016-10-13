package de.hhu.bsinfo.dxram.term;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;

public class DXRAMContext {

	private DXRAMServiceAccessor m_serviceAccessor;

	public final Script script;
	public final Terminal term;
	public final Util util;

	public DXRAMContext(final DXRAMServiceAccessor p_serviceAccessor, final TerminalDelegate p_terminalDelegate,
			final TerminalScriptEngine p_scriptEngine) {
		m_serviceAccessor = p_serviceAccessor;

		script = new Script(p_scriptEngine);
		term = new Terminal(p_terminalDelegate);
		util = new Util();
	}

	public <T extends AbstractDXRAMService> T service(final Class<T> p_class) {
		return m_serviceAccessor.getService(p_class);
	}

	public <T extends AbstractTerminalCommand> T cmd(final String p_cmdName) {
		return null;
	}

	public static class Script {

		private TerminalScriptEngine m_scriptEngine;

		public Script(final TerminalScriptEngine p_scriptEngine) {
			m_scriptEngine = p_scriptEngine;
		}

		public void load(final String p_path) {
			m_scriptEngine.loadScriptFile(p_path);
		}
	}

	public static class Terminal {

		private TerminalDelegate m_terminalDelegate;

		public Terminal(final TerminalDelegate p_delegate) {
			m_terminalDelegate = p_delegate;
		}

		public void exit() {
			m_terminalDelegate.exitTerminal();
		}

		public String input(final String p_header) {
			return m_terminalDelegate.promptForUserInput(p_header);
		}

		public boolean areYouSure() {
			return m_terminalDelegate.areYouSure();
		}

		public void print(final String p_str) {
			m_terminalDelegate.println(p_str);
		}

		public void print(final String p_str, final TerminalColor p_color) {
			m_terminalDelegate.println(p_str, p_color);
		}

		public void print(final String p_str, final TerminalColor p_color, final TerminalColor p_backgroundColor,
				final TerminalStyle p_style) {
			m_terminalDelegate.println(p_str, p_color, p_backgroundColor, p_style);
		}

		public void clear() {
			m_terminalDelegate.clear();
		}
	}

	public static class Util {

		public Util() {

		}

		public void sleep(final int p_timeSec) {
			try {
				Thread.sleep(p_timeSec * 1000);
			} catch (final InterruptedException ignored) {
			}
		}
	}
}
