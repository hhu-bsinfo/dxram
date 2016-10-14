
package de.hhu.bsinfo.dxram.term;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

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

	private ScriptTerminalContext m_terminalContext;
	private ScriptContext m_terminalScriptContext;
	private List<String> m_terminalScriptCommands = new ArrayList<>();

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
	ScriptContext getTerminalScriptContext() {
		return m_terminalScriptContext;
	}

	/**
	 * Get all registered commands. This is used by the service, only.
	 *
	 * @return Map of registered commands.
	 */
	List<String> getRegisteredCommands() {
		return m_terminalScriptCommands;
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
		m_terminalContext = new ScriptTerminalContext(m_scriptEngine, m_terminalScriptCommands);

		m_terminalScriptContext.bind("dxterm", m_terminalContext);

		String scriptFolder = p_settings.getValue(TerminalConfigurationValues.Component.TERM_CMD_SCRIPT_FOLDER);

		if (!scriptFolder.isEmpty()) {
			loadTerminalScripts(scriptFolder);
		}

		return true;
	}

	@Override
	protected boolean shutdownComponent() {

		for (String cmd : m_terminalScriptCommands) {
			m_scriptEngine.destroyContext(cmd);
		}

		m_terminalScriptCommands.clear();

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
					if (ctx != null && ctx.load(file.getAbsolutePath())) {

						if (assertFunctionExists(ctx, "help") && assertFunctionExists(ctx, "exec")) {

							m_terminalScriptCommands.add(name);
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
