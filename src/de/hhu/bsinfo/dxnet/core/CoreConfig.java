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

package de.hhu.bsinfo.dxnet.core;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Common configuration values for the core parts of the network subsystem
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 28.07.2017
 */
public class CoreConfig {
    // don't expose, not a configurable attribute
    private short m_ownNodeId = NodeID.INVALID_ID;

    @Expose
    private int m_numMessageHandlerThreads = 2;

    @Expose
    private int m_requestMapSize = 1048576;

    @Expose
    private boolean m_useStaticExporterPool = true;

    @Expose
    private String m_device = "Ethernet";

    /**
     * Default constructor
     */
    public CoreConfig() {

    }

    /**
     * Get the node id of the current node
     */
    public short getOwnNodeId() {
        return m_ownNodeId;
    }

    /**
     * Set the node id of the current node
     */
    public void setOwnNodeId(final short p_nodeId) {
        m_ownNodeId = p_nodeId;
    }

    /**
     * Number of threads to spawn for handling incoming and assembled network messages
     */
    public int getNumMessageHandlerThreads() {
        return m_numMessageHandlerThreads;
    }

    /**
     * Size of the map that stores outstanding requests and maps them to their incoming responses
     */
    public int getRequestMapSize() {
        return m_requestMapSize;
    }

    /**
     * The exporter pool type. True if static, false if dynamic. Static is recommended for less than 1000 actively message sending threads.
     */
    public boolean getExporterPoolType() {
        return m_useStaticExporterPool;
    }

    /**
     * The device name (Ethernet, Infiniband or Loopback)
     */
    public String getDevice() {
        return m_device;
    }
}
