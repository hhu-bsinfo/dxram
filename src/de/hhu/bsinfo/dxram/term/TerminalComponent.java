
package de.hhu.bsinfo.dxram.term;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.script.ScriptContext;
import de.hhu.bsinfo.dxram.script.ScriptEngineComponent;

/**
 * Component providing a separate script context and data/commands for an interactive terminal to be run on a node.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
 */
public class TerminalComponent extends AbstractDXRAMComponent {

	// configuration values
	@Expose
	private String m_termCmdScriptFolder = "script/term";

	// dependent components
	private LoggerComponent m_logger;
	private ScriptEngineComponent m_scriptEngine;

	private ScriptTerminalContext m_terminalContext;
	private ScriptContext m_terminalScriptContext;
	private Map<String, ScriptContext> m_terminalScriptCommands = new HashMap<>();

	/**
	 * Constructor
	 */
	public TerminalComponent() {
		super(DXRAMComponentOrder.Init.TERMINAL, DXRAMComponentOrder.Shutdown.TERMINAL);
	}

	/**
	 * Get the script context of the terminal
	 *
	 * @return ScriptContext
	 */
	ScriptContext getScriptContext() {
		return m_terminalScriptContext;
	}

	/**
	 * Get the context exposed inside the script context to be used in the terminal
	 *
	 * @return ScriptTerminalContext
	 */
	ScriptTerminalContext getScriptTerminalContext() {
		return m_terminalContext;
	}

	/**
	 * Get all registered commands. This is used by the service, only.
	 *
	 * @return Map of registered commands.
	 */
	Map<String, ScriptContext> getRegisteredCommands() {
		return m_terminalScriptCommands;
	}

	/**
	 * Reload the terminal scripts
	 */
	void reloadTerminalScripts() {
		if (!m_termCmdScriptFolder.isEmpty()) {
			unloadTerminalScripts();
			loadTerminalScripts(m_termCmdScriptFolder);
		}
	}

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_logger = p_componentAccessor.getComponent(LoggerComponent.class);
        m_scriptEngine = p_componentAccessor.getComponent(ScriptEngineComponent.class);
    }

	@Override
	protected boolean initComponent(final DXRAMContext.EngineSettings p_engineEngineSettings) {
		// create script context for terminal
		m_terminalScriptContext = m_scriptEngine.createContext("terminal");
		m_terminalContext = new ScriptTerminalContext(m_scriptEngine, this);

		m_terminalScriptContext.bind("dxterm", m_terminalContext);

		// initial load
		reloadTerminalScripts();

		return true;
	}

	@Override
	protected boolean shutdownComponent() {

		unloadTerminalScripts();

		m_terminalContext = null;
		m_scriptEngine = null;
		m_logger = null;

		return true;
	}

	/**
	 * Load all terminal scripts from a certain folder
	 *
	 * @param p_path Path to the folder with terminal scripts
	 */
	private void loadTerminalScripts(final String p_path) {

		// #if LOGGER >= INFO
		m_logger.info(getClass(), "Loading terminal scripts from directory '" + p_path + "'...");
		// #endif /* LOGGER >= INFO */

		File dir = new File(p_path);

		File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
			for (File file : directoryListing) {
				if (file.getName().endsWith(".js")) {
					// #if LOGGER >= DEBUG
					m_logger.debug(getClass(), "Loading terminal script '" + file.getName() + "'.");
					// #endif /* LOGGER >= DEBUG */

					String name = file.getName().split("\\.")[0];

					ScriptContext ctx = m_scriptEngine.createContext(name);
					if (ctx != null) {

						if (ctx.load(file.getAbsolutePath()) && assertFunctionExists(ctx, "help")
								&& assertFunctionExists(ctx, "exec")
								&& assertFunctionExists(ctx, "imports")) {

							// bind terminal context
							ctx.bind("dxterm", m_terminalContext);

							m_terminalScriptCommands.put(name, ctx);
						} else {
							m_scriptEngine.destroyContext(ctx);
						}
					}
				}
			}
		} else {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "List directory contents of script directory '" + p_path + "' failed.");
			// #endif /* LOGGER >= ERROR */
		}
	}

	/**
	 * Unload all loaded terminal scripts and destroy the script contexts
	 */
	private void unloadTerminalScripts() {
		for (ScriptContext ctx : m_terminalScriptCommands.values()) {
			m_scriptEngine.destroyContext(ctx);
		}

		m_terminalScriptCommands.clear();
	}

	/**
	 * Assert if a function in a script exists.
	 *
	 * @param p_ctx  Script context to search the function for
	 * @param p_name Name of the function to search
	 * @return True if function found, false otherwise
	 */
	private boolean assertFunctionExists(final ScriptContext p_ctx, final String p_name) {
		if (!p_ctx.functionExists(p_name)) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Loading terminal script '" + p_ctx.getName()
					+ "' failed: missing '" + p_name + "' function.");
			// #endif /* LOGGER >= ERROR */

			return false;
		}

		return true;
	}
}
