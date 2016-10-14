package de.hhu.bsinfo.dxram.script;

import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;
import java.io.FileNotFoundException;
import java.io.FileReader;

import de.hhu.bsinfo.dxram.logger.LoggerComponent;

/**
 * Created by nothaas on 10/14/16.
 */
public class ScriptContext {

	private ScriptDXRAMContext m_scriptEngineContext;
	private ScriptEngine m_scriptEngine;
	private String m_name;
	private javax.script.ScriptContext m_scriptContext;

	private LoggerComponent m_logger;

	ScriptContext(final ScriptDXRAMContext p_scriptEngineContext, final ScriptEngine p_scriptEngine,
			final LoggerComponent p_logger, final String p_name) {
		m_scriptEngine = p_scriptEngine;
		m_scriptEngineContext = p_scriptEngineContext;
		m_name = p_name;
		m_logger = p_logger;

		m_scriptContext = new SimpleScriptContext();
		setDefaultBindings(m_scriptContext);
	}

	public String getName() {
		return m_name;
	}

	public boolean load(final String p_path) {

		m_scriptEngine.setContext(m_scriptContext);

		try {
			try {
				m_scriptEngine.eval(new FileReader(p_path));
			} catch (final ScriptException e) {
				// #if LOGGER >= ERROR
				m_logger.error(getClass(), "Loading script file '" + p_path + "' failed: " + e.getMessage());
				// #endif /* LOGGER >= ERROR */
				return false;
			} catch (final FileNotFoundException e) {
				// #if LOGGER >= ERROR
				m_logger.error(getClass(), "Loading script file '" + p_path + "' failed: file not found.");
				// #endif /* LOGGER >= ERROR */
				return false;
			}
		} catch (final Exception e) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), e.getMessage());
			// #endif /* LOGGER >= ERROR */

			return false;
		}

		return true;
	}

	public boolean bind(final String p_key, final Object p_val) {

		m_scriptEngine.setContext(m_scriptContext);
		m_scriptEngine.put(p_key, p_val);

		return true;
	}

	public boolean eval(final String p_text) {

		m_scriptEngine.setContext(m_scriptContext);

		try {
			try {
				m_scriptEngine.eval(p_text);
			} catch (final ScriptException e) {
				// #if LOGGER >= ERROR
				m_logger.error(getClass(), "Evaluating '" + p_text + "' failed: " + e.getMessage());
				// #endif /* LOGGER >= ERROR */
				return false;
			}
		} catch (final Exception e) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), e.getMessage());
			// #endif /* LOGGER >= ERROR */

			return false;
		}

		return true;
	}

	public boolean functionExists(final String p_name) {

		m_scriptEngine.setContext(m_scriptContext);

		try {
			return (Boolean) m_scriptEngine.eval("typeof " + p_name
					+ " === 'function' ? java.lang.Boolean.TRUE : java.lang.Boolean.FALSE");
		} catch (final ScriptException e) {
			return false;
		}
	}

	public Object call(final String p_name, Object... args) {
		m_scriptEngine.setContext(m_scriptContext);

		Invocable inv = (Invocable) m_scriptEngine;

		try {
			return inv.invokeFunction(p_name, args);
		} catch (final ScriptException e) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Calling '" + p_name + "' failed: " + e.getMessage());
			// #endif /* LOGGER >= ERROR */
		} catch (final NoSuchMethodException e) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Calling '" + p_name + "' failed, function not available");
			// #endif /* LOGGER >= ERROR */
		}

		return null;
	}

	// -------------------------------------------------------------------------------------------------------

	private void setDefaultBindings(final javax.script.ScriptContext p_ctx) {
		m_scriptEngine.setContext(p_ctx);
		Bindings bindings = m_scriptEngine.createBindings();

		bindings.put("dxram", m_scriptEngineContext);

		p_ctx.setBindings(bindings, javax.script.ScriptContext.ENGINE_SCOPE);
	}
}
