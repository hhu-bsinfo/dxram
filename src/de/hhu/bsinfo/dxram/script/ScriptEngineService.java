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

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Service exposing the java script engine
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 14.10.2016
 */
public class ScriptEngineService extends AbstractDXRAMService {

    // dependent components
    private ScriptEngineComponent m_scriptEngine;

    /**
     * Constructor
     */
    public ScriptEngineService() {
        super("script");
    }

    /**
     * Load a script file to the java script engine context of another node
     *
     * @param p_path
     *     Path of the java script file to load
     * @param p_nodeId
     *     Node id of the node to load the script file on
     * @return True if successful, false otherwise
     */
    public static boolean load(final String p_path, final short p_nodeId) {

        // TODO send message to other node
        return false;
    }

    /**
     * Evaluate the text in the java script engine of another node
     *
     * @param p_text
     *     Text to evaluate in the java script engine
     * @param p_nodeId
     *     Node id of the node to evaluate the text on
     * @return True if successful, false on error
     */
    public static boolean eval(final String p_text, final short p_nodeId) {

        // TODO send message to other node
        return false;
    }

    /**
     * Load a script file into the context of the java script engine
     *
     * @param p_path
     *     Path to the java script file to load
     * @return True if successful, false otherwise.
     */
    public boolean load(final String p_path) {

        return m_scriptEngine.getContext().load(p_path);
    }

    /**
     * Evaluate the text in the java script engine.
     *
     * @param p_text
     *     Text to evaluate in the java script engine
     * @return True if successful, false on error
     */
    public boolean eval(final String p_text) {

        return m_scriptEngine.getContext().eval(p_text);
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_scriptEngine = p_componentAccessor.getComponent(ScriptEngineComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }
}
