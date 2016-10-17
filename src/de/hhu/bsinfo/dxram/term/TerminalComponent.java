
package de.hhu.bsinfo.dxram.term;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.script.ScriptContext;
import de.hhu.bsinfo.dxram.script.ScriptEngineComponent;

/**
 * Component providing data/commands for an interactive terminal to be run on a node.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
 */
public class TerminalComponent extends AbstractDXRAMComponent {

	private LoggerComponent m_logger;
	private ScriptEngineComponent m_scriptEngine;

	private String m_terminalScriptFolder;
	private ScriptTerminalContext m_terminalContext;
	private ScriptContext m_terminalScriptContext;
	private Map<String, ScriptContext> m_terminalScriptCommands = new HashMap<>();

	/**
	 * Constructor
	 *
	 * @param p_priorityInit     Priority for initialization of this component.
	 *                           When choosing the order, consider component dependencies here.
	 * @param p_priorityShutdown Priority for shutting down this component.
	 *                           When choosing the order, consider component dependencies here.
	 */
	public TerminalComponent(final int p_priorityInit, final int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);

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
		if (!m_terminalScriptFolder.isEmpty()) {
			unloadTerminalScripts();
			loadTerminalScripts(m_terminalScriptFolder);
		}
	}

	@Override
	protected void registerDefaultSettingsComponent(final Settings p_settings) {
		p_settings.setDefaultValue(TerminalConfigurationValues.Component.TERM_CMD_SCRIPT_FOLDER);
	}

	@Override
	protected boolean initComponent(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {
		m_logger = getDependentComponent(LoggerComponent.class);
		m_scriptEngine = getDependentComponent(ScriptEngineComponent.class);

		// create script context for terminal
		m_terminalScriptContext = m_scriptEngine.createContext("terminal");
		m_terminalContext = new ScriptTerminalContext(m_scriptEngine, this);

		m_terminalScriptContext.bind("dxterm", m_terminalContext);

		m_terminalScriptFolder = p_settings.getValue(TerminalConfigurationValues.Component.TERM_CMD_SCRIPT_FOLDER);

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
								&& assertFunctionExists(ctx, "exec")) {

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
