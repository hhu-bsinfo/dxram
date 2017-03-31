/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

import java.io.FileNotFoundException;
import java.io.FileReader;

import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Script context wrapper for java script engine.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 14.10.2016
 */
public class ScriptContext {

    private static final Logger LOGGER = LogManager.getFormatterLogger(ScriptContext.class.getSimpleName());

    private ScriptDXRAMContext m_scriptEngineContext;
    private ScriptEngine m_scriptEngine;
    private String m_name;
    private javax.script.ScriptContext m_scriptContext;

    /**
     * Constructor
     *
     * @param p_scriptEngineContext
     *     DXRAM script context to be exposed to the java script engine
     * @param p_scriptEngine
     *     The (java) script engine to use
     * @param p_name
     *     Name of this context
     */
    ScriptContext(final ScriptDXRAMContext p_scriptEngineContext, final ScriptEngine p_scriptEngine, final String p_name) {
        m_scriptEngine = p_scriptEngine;
        m_scriptEngineContext = p_scriptEngineContext;
        m_name = p_name;

        m_scriptContext = new SimpleScriptContext();
        initContext(m_scriptContext);
    }

    /**
     * Get the name/identifier of this context
     *
     * @return Name of this context
     */
    public String getName() {
        return m_name;
    }

    /**
     * Load a script file into this context
     *
     * @param p_path
     *     Path to the script file
     * @return True if loading successful, false on error.
     */
    public boolean load(final String p_path) {

        m_scriptEngine.setContext(m_scriptContext);

        try {
            try {
                m_scriptEngine.eval(new FileReader(p_path));
            } catch (final ScriptException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Loading script file '%s' failed: %s", p_path, e.getMessage());
                // #endif /* LOGGER >= ERROR */
                return false;
            } catch (final FileNotFoundException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Loading script file '%s' failed: file not found", p_path);
                // #endif /* LOGGER >= ERROR */
                return false;
            }
        } catch (final Exception e) {
            // #if LOGGER >= ERROR
            LOGGER.error(e);
            // #endif /* LOGGER >= ERROR */

            return false;
        }

        return true;
    }

    /**
     * Bind an object to this context.
     *
     * @param p_key
     *     Key for the binding
     * @param p_val
     *     Object to bind
     * @return True if sucessful, false on error
     */
    public boolean bind(final String p_key, final Object p_val) {

        m_scriptEngine.setContext(m_scriptContext);
        m_scriptEngine.put(p_key, p_val);

        return true;
    }

    /**
     * Evaluate the given string for this context
     *
     * @param p_text
     *     Text to evaluate
     * @return True on success, false on error
     */
    public boolean eval(final String p_text) {

        m_scriptEngine.setContext(m_scriptContext);

        try {
            try {
                m_scriptEngine.eval(p_text);
            } catch (final ScriptException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Evaluating '%s' failed: %s", p_text, e.getMessage());
                // #endif /* LOGGER >= ERROR */
                return false;
            }
        } catch (final Exception e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Exception", e);
            // #endif /* LOGGER >= ERROR */

            return false;
        }

        return true;
    }

    /**
     * Check if a specific function exists in this context
     *
     * @param p_name
     *     Name of the function to check
     * @return True if function exists, false otherwise
     */
    public boolean functionExists(final String p_name) {

        m_scriptEngine.setContext(m_scriptContext);

        try {
            return (Boolean) m_scriptEngine.eval("typeof " + p_name + " === 'function' ? java.lang.Boolean.TRUE : java.lang.Boolean.FALSE");
        } catch (final ScriptException ignored) {
            return false;
        }
    }

    /**
     * Call a function that is defined in this context
     *
     * @param p_name
     *     Name of the function
     * @param p_args
     *     Arguments for the function
     * @return Return value of the function (and also null on error but can be return value of function as welL)
     */
    public Object call(final String p_name, final Object... p_args) {
        m_scriptEngine.setContext(m_scriptContext);

        Invocable inv = (Invocable) m_scriptEngine;

        try {
            return inv.invokeFunction(p_name, p_args);
        } catch (final ScriptException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Calling '%s' failed: %s", p_name, e.getMessage());
            // #endif /* LOGGER >= ERROR */
        } catch (final NoSuchMethodException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Calling '%s' failed, function not available: %s", p_name, e.getMessage());
            // #endif /* LOGGER >= ERROR */
        }

        return null;
    }

    // -------------------------------------------------------------------------------------------------------

    /**
     * Set the default bindings for this context i.e. bind the dxram context to it.
     *
     * @param p_ctx
     *     Java script context
     */
    private void initContext(final javax.script.ScriptContext p_ctx) {
        m_scriptEngine.setContext(p_ctx);
        Bindings bindings = m_scriptEngine.createBindings();

        bindings.put("dxram", m_scriptEngineContext);

        p_ctx.setBindings(bindings, javax.script.ScriptContext.ENGINE_SCOPE);

        // this extends the java script engine and adds a lot of useful things like better java
        // interoperability, package loading etc
        try {
            m_scriptEngine.eval("load(\"nashorn:mozilla_compat.js\")");
        } catch (final ScriptException ignored) {
            // ignored
        }
    }
}
