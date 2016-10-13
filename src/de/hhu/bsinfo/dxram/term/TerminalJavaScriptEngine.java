package de.hhu.bsinfo.dxram.term;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

class TerminalJavaScriptEngine implements TerminalScriptEngine {

	private ScriptEngineManager m_scriptEngineManager;
	private ScriptEngine m_scriptEngine;
	private DXRAMContext m_dxramContext;

	private Map<String, ScriptContext> m_scriptCommands = new HashMap<>();

	public TerminalJavaScriptEngine() {
		m_scriptEngineManager = new ScriptEngineManager();
		m_scriptEngine = m_scriptEngineManager.getEngineByName("JavaScript");
	}

	@Override
	public String getHelp() {
		return "A context called 'dxram' is exposed by the terminal which contains further sub-contexts.\n"
				+ "Outline:\nTODO";
	}

	@Override
	public boolean setupDXRAMContext(final DXRAMContext p_dxramContext) {

		m_scriptEngine.put("dxram", p_dxramContext);
		m_scriptEngine.put("exit", "dxram.term.exit()");

		return true;
	}

	@Override
	public boolean loadScriptFile(final String p_path) {
		try {
			m_scriptEngine.eval(new FileReader(p_path));
		} catch (final Exception e) {
			System.out.println("Loading script file '" + p_path + "' failed: " + e.getMessage());
			return false;
		}

		return true;
	}

	@Override
	public boolean loadScriptCommandFile(final String p_path) {

		ScriptContext ctx = new SimpleScriptContext();

		//		m_scriptEngine.eval(new FileReader(p_path), ctx);
		//
		//		m_scriptEngine.eval(p_path, ctx);

		// TODO use different contexts to load the script commands to from command script files

		//		function getName() {
		//			return "nodelist"
		//		}
		//
		//		function getDescription() {
		//			return "List all nodes"
		//		}
		//
		//		function execute() {
		//			var boot = dxram.service("boot");
		//
		//			// TODO
		//		}

		return true;
	}

	@Override
	public boolean evaluate(final String p_text) {

		try {
			m_scriptEngine.eval(p_text);
		} catch (final ScriptException e) {
			System.out.println("Evaluating '" + p_text + "' failed: " + e.getMessage());
			return false;
		}

		return true;
	}

	public boolean executeScriptCommand(final String p_name) {
		return false;
	}
}
