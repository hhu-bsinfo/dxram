package de.hhu.bsinfo.dxram.script;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.overlay.BarrierID;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.menet.NodeID;

/**
 * Script engine component creating a java script engine instance with access to different contexts
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 14.10.16
 */
public class ScriptEngineComponent extends AbstractDXRAMComponent implements ScriptDXRAMContext {

	private ScriptEngineManager m_scriptEngineManager;
	private ScriptEngine m_scriptEngine;

	private ScriptContext m_defaultScriptContext;
	private Map<String, ScriptContext> m_scriptContexts = new HashMap<>();

	private ScriptDXRAMContext m_scriptEngineContext;

	private LoggerComponent m_logger;

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

	/**
	 * Create a new context.
	 *
	 * @param p_name Name of the context
	 * @return Newly created context or null if a context with the name already exists
	 */
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

	/**
	 * Destroy a context by name
	 *
	 * @param p_name Name of the context to destroy
	 */
	public void destroyContext(final String p_name) {
		ScriptContext ctx = getContext(p_name);

		if (ctx != null) {
			m_scriptContexts.remove(ctx.getName());
		}
	}

	/**
	 * Destroy a context
	 *
	 * @param p_ctx Context to destroy
	 */
	public void destroyContext(final ScriptContext p_ctx) {
		m_scriptContexts.remove(p_ctx.getName());
	}

	/**
	 * Get the default script context
	 *
	 * @return Default script context
	 */
	public ScriptContext getContext() {
		return m_defaultScriptContext;
	}

	/**
	 * Get a context by name
	 *
	 * @param p_name Name of the context
	 * @return Context assigned to the specified name or null if no context for that name exists
	 */
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
	public String shortToHexStr(final short p_val) {
		return NodeID.toHexString(p_val);
	}

	@Override
	public String intToHexStr(final int p_val) {
		return BarrierID.toHexString(p_val);
	}

	@Override
	public String longToHexStr(final long p_val) {
		return ChunkID.toHexString(p_val);
	}

	@Override
	public long longStrToLong(final String p_str) {

		String str = p_str.toLowerCase();

		if (str.length() > 1) {
			String tmp = str.substring(0, 2);
			// oh java...no unsigned, why?
			switch (tmp) {
				case "0x":
					return (new BigInteger(str.substring(2), 16)).longValue();
				case "0b":
					return (new BigInteger(str.substring(2), 2)).longValue();
				case "0o":
					return (new BigInteger(str.substring(2), 8)).longValue();
				default:
					break;
			}
		}

		return java.lang.Long.parseLong(str);
	}

	@Override
	public NodeRole nodeRole(final String p_str) {
		return NodeRole.toNodeRole(p_str);
	}

	@Override
	public void sleep(final int p_timeMs) {
		try {
			Thread.sleep(p_timeMs);
		} catch (final InterruptedException ignored) {
		}
	}

	@Override
	public long cid(final short p_nid, final long p_lid) {
		return ChunkID.getChunkID(p_nid, p_lid);
	}

	@Override
	public short nidOfCid(final long p_cid) {
		return ChunkID.getCreatorID(p_cid);
	}

	@Override
	public long lidOfCid(final long p_cid) {
		return ChunkID.getLocalID(p_cid);
	}

	@Override
	public Chunk newChunk() {
		return new Chunk();
	}

	@Override
	public Chunk newChunk(final int p_bufferSize) {
		return new Chunk(p_bufferSize);
	}

	@Override
	public Chunk newChunk(final ByteBuffer p_buffer) {
		return new Chunk(p_buffer);
	}

	@Override
	public Chunk newChunk(final long p_id) {
		return new Chunk(p_id);
	}

	@Override
	public Chunk newChunk(final long p_id, final int p_bufferSize) {
		return new Chunk(p_id, p_bufferSize);
	}

	@Override
	public Chunk newChunk(final long p_id, final ByteBuffer p_buffer) {
		return new Chunk(p_id, p_buffer);
	}

	@Override
	public DataStructure newDataStructure(final String p_className) {
		Class<?> clazz;
		try {
			clazz = Class.forName(p_className);
		} catch (final ClassNotFoundException e) {
			m_logger.error(getClass(), "Cannot find class with name " + p_className);
			return null;
		}

		if (!DataStructure.class.isAssignableFrom(clazz)) {
			m_logger.error(getClass(), "Class " + p_className + " is not implementing the DataStructure interface");
			return null;
		}

		DataStructure dataStructure;
		try {
			dataStructure = (DataStructure) clazz.getConstructor().newInstance();
		} catch (final InstantiationException | IllegalAccessException
				| InvocationTargetException | NoSuchMethodException e) {
			m_logger.error(getClass(), "Creating instance of " + p_className + " failed: " + e.getMessage());
			return null;
		}

		return dataStructure;
	}

	@Override
	public String readFile(final String p_path) {
		try {
			return new String(Files.readAllBytes(Paths.get(p_path)));
		} catch (final IOException ignored) {
			return null;
		}
	}
}
