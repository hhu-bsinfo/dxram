package de.hhu.bsinfo.dxnet.core;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.utils.NodeID;

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
    private boolean m_infiniband = false;

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
     * True if you want to use the infiniband interface, false for ethernet
     */
    public boolean getInfiniband() {
        return m_infiniband;
    }
}
