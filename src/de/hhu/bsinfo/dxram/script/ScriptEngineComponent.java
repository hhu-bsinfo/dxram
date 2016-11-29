/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.script;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Script engine component creating a java script engine instance with access to different contexts
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 14.10.2016
 */
public class ScriptEngineComponent extends AbstractDXRAMComponent implements ScriptDXRAMContext {

    private static final Logger LOGGER = LogManager.getFormatterLogger(ScriptEngineComponent.class.getSimpleName());

    // private state
    private ScriptEngineManager m_scriptEngineManager;
    private ScriptEngine m_scriptEngine;

    private ScriptContext m_defaultScriptContext;
    private Map<String, ScriptContext> m_scriptContexts = new HashMap<>();

    private ScriptDXRAMContext m_scriptEngineContext;

    /**
     * Constructor
     */
    public ScriptEngineComponent() {
        super(DXRAMComponentOrder.Init.SCRIPT, DXRAMComponentOrder.Shutdown.SCRIPT);
    }

    /**
     * Get the default script context
     *
     * @return Default script context
     */
    public ScriptContext getContext() {
        return m_defaultScriptContext;
    }

    @Override
    protected boolean isEngineAccessor() {
        return true;
    }

    /**
     * Create a new context.
     *
     * @param p_name
     *     Name of the context
     * @return Newly created context or null if a context with the name already exists
     */
    public ScriptContext createContext(final String p_name) {

        if (m_scriptContexts.containsKey(p_name)) {
            // #if LOGGER >= ERROR
            LOGGER.error("Cannot create new context '%s', name already exists", p_name);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        ScriptContext ctx = new ScriptContext(this, m_scriptEngine, p_name);
        m_scriptContexts.put(p_name, ctx);

        return ctx;
    }

    /**
     * Destroy a context by name
     *
     * @param p_name
     *     Name of the context to destroy
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
     * @param p_ctx
     *     Context to destroy
     */
    public void destroyContext(final ScriptContext p_ctx) {
        m_scriptContexts.remove(p_ctx.getName());
    }

    // -------------------------------------------------------------------------------------------------------

    /**
     * Get a context by name
     *
     * @param p_name
     *     Name of the context
     * @return Context assigned to the specified name or null if no context for that name exists
     */
    public ScriptContext getContext(final String p_name) {

        ScriptContext ctx = m_scriptContexts.get(p_name);

        if (ctx == null) {
            // #if LOGGER >= ERROR
            LOGGER.error("Script context '%s' does not exist", p_name);
            // #endif /* LOGGER >= ERROR */
        }

        return ctx;
    }

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

    // -------------------------------------------------------------------------------------------------------

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
                    return new BigInteger(str.substring(2), 16).longValue();
                case "0b":
                    return new BigInteger(str.substring(2), 2).longValue();
                case "0o":
                    return new BigInteger(str.substring(2), 8).longValue();
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
    public DataStructure newDataStructure(final String p_className) {
        Class<?> clazz;
        try {
            clazz = Class.forName(p_className);
        } catch (final ClassNotFoundException ignored) {
            LOGGER.error("Cannot find class with name %s", p_className);
            return null;
        }

        if (!DataStructure.class.isAssignableFrom(clazz)) {
            LOGGER.error("Class %s is not implementing the DataStructure interface", p_className);
            return null;
        }

        DataStructure dataStructure;
        try {
            dataStructure = (DataStructure) clazz.getConstructor().newInstance();
        } catch (final InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            LOGGER.error("Creating instance of %s failed: %s", p_className, e.getMessage());
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

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        // no dependencies
    }

    @Override
    protected boolean initComponent(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        m_scriptEngineManager = new ScriptEngineManager();
        m_scriptEngine = m_scriptEngineManager.getEngineByName("JavaScript");

        // create default context
        m_defaultScriptContext = new ScriptContext(m_scriptEngineContext, m_scriptEngine, "default");

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

        return true;
    }
}
