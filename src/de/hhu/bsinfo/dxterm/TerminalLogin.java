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

package de.hhu.bsinfo.dxterm;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import de.hhu.bsinfo.utils.NodeID;

/**
 * Login object sent by the server to the thin client on login
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class TerminalLogin implements Serializable {
    private byte m_sessionId = -1;
    private short m_nodeId = NodeID.INVALID_ID;
    private List<String> m_cmdNames = Collections.emptyList();

    /**
     * Constructor
     */
    public TerminalLogin() {

    }

    /**
     * Constructor
     *
     * @param p_sessionId
     *         Session id assigned by the server
     * @param p_nodeId
     *         Node id of the DXRAM peer instance the client connected to
     * @param p_cmdNames
     *         List of names of available commands (for auto completion)
     */
    public TerminalLogin(final byte p_sessionId, final short p_nodeId, final List<String> p_cmdNames) {
        m_sessionId = p_sessionId;
        m_nodeId = p_nodeId;
        m_cmdNames = p_cmdNames;
    }

    /**
     * Get the session id
     */
    public byte getSessionId() {
        return m_sessionId;
    }

    /**
     * Get the node id of the DXRAM peer the terminal server is running on
     */
    public short getNodeId() {
        return m_nodeId;
    }

    /**
     * Get the list of available commands
     */
    public List<String> getCmdNames() {
        return m_cmdNames;
    }
}
