package de.hhu.bsinfo.dxram.script;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;

/**
 * Created by nothaas on 10/14/16.
 */
public class ScriptEngineService extends AbstractDXRAMService {

	private ScriptEngineComponent m_scriptEngine;

	/**
	 * Constructor
	 */
	public ScriptEngineService() {
		super("script");
	}

	public boolean load(final String p_path) {

		return m_scriptEngine.getContext().load(p_path);
	}

	public boolean load(final String p_path, final short p_nodeId) {

		// TODO send message to other node
		return false;
	}

	public boolean eval(final String p_text) {

		return m_scriptEngine.getContext().eval(p_text);
	}

	public boolean eval(final String p_text, final short p_nodeId) {

		// TODO send message to other node
		return false;
	}

	@Override
	protected void registerDefaultSettingsService(final Settings p_settings) {

	}

	@Override
	protected boolean startService(final DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {

		m_scriptEngine = getComponent(ScriptEngineComponent.class);

		return true;
	}

	@Override
	protected boolean shutdownService() {

		m_scriptEngine = null;

		return true;
	}
}
