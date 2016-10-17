package de.hhu.bsinfo.dxram.script;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.menet.NodeID;

/**
 * Created by nothaas on 10/14/16.
 */
public class ScriptEngineComponent extends AbstractDXRAMComponent implements ScriptDXRAMContext {

	private ScriptEngineManager m_scriptEngineManager;
	private ScriptEngine m_scriptEngine;

	private ScriptContext m_defaultScriptContext;
	private Map<String, ScriptContext> m_scriptContexts = new HashMap<>();

	private ScriptDXRAMContext m_scriptEngineContext;

	private LoggerComponent m_logger;
	private AbstractBootComponent m_boot;

	/**
	 * Constructor
	 *
	 * @param p_priorityInit     Priority for initialization of this component.
	 *                           When choosing the order, consider component dependencies here.
	 * @param p_priorityShutdown Priority for shutting down this component.
	 */
	public ScriptEngineComponent(final int p_priorityInit, final int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

	public ScriptContext createContext(final String p_name) {

		if (m_scriptContexts.containsKey(p_name)) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Cannot create new context '" + p_name + "', name already exists.");
			// #endif /* LOGGER >= ERROR */
			return null;
		}

		ScriptContext ctx = new ScriptContext(this, m_scriptEngine, m_logger, p_name);
		m_scriptContexts.put(p_name, ctx);

		return ctx;
	}

	public void destroyContext(final String p_name) {
		ScriptContext ctx = getContext(p_name);

		if (ctx != null) {
			m_scriptContexts.remove(ctx.getName());
		}
	}

	public void destroyContext(final ScriptContext p_ctx) {
		m_scriptContexts.remove(p_ctx.getName());
	}

	public ScriptContext getContext() {
		return m_defaultScriptContext;
	}

	public ScriptContext getContext(final String p_name) {

		ScriptContext ctx = m_scriptContexts.get(p_name);

		if (ctx == null) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Script context '" + p_name + "' does not exist");
			// #endif /* LOGGER >= ERROR */
		}

		return ctx;
	}

	// -------------------------------------------------------------------------------------------------------

	@Override
	protected void registerDefaultSettingsComponent(final Settings p_settings) {
		p_settings.setDefaultValue(ScriptConfigurationValues.Component.AUTOSTART_SCRIPT);
	}

	@Override
	protected boolean initComponent(final DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {

		m_logger = getDependentComponent(LoggerComponent.class);
		m_boot = getDependentComponent(AbstractBootComponent.class);

		m_scriptEngineManager = new ScriptEngineManager();
		m_scriptEngine = m_scriptEngineManager.getEngineByName("JavaScript");

		// create default context
		m_defaultScriptContext = new ScriptContext(m_scriptEngineContext, m_scriptEngine, m_logger, "default");

		// TODO autostart script

		return true;
	}

	@Override
	protected boolean shutdownComponent() {

		m_defaultScriptContext = null;
		m_scriptContexts.clear();

		m_scriptEngine = null;
		m_scriptEngineManager = null;

		m_scriptEngineContext = null;

		m_boot = null;
		m_logger = null;

		return true;
	}

	// -------------------------------------------------------------------------------------------------------

	@Override
	public void list() {
		List<String> list = getParentEngine().getServiceShortNames();

		String str = "";

		for (String item : list) {
			str += item + ", ";
		}

		System.out.println(str.substring(0, str.length() - 2));
	}

	@Override
	public AbstractDXRAMService service(final String p_serviceName) {
		return getParentEngine().getService(p_serviceName);
	}

	@Override
	public String nidhexstr(final short nodeId) {
		return NodeID.toHexString(nodeId);
	}

	@Override
	public String cidhexstr(final long chunkId) {
		return ChunkID.toHexString(chunkId);
	}

	@Override
	public NodeRole noderole(final String p_str) {
		return NodeRole.toNodeRole(p_str);
	}

	@Override
	public void sleep(final int p_timeMs) {
		try {
			Thread.sleep(p_timeMs);
		} catch (final InterruptedException ignored) {
		}
	}
}
