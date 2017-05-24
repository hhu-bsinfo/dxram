package de.hhu.bsinfo.dxram.net;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.utils.unit.StorageUnit;
import de.hhu.bsinfo.utils.unit.TimeUnit;

/**
 * Config for the NetworkComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class NetworkComponentConfig extends DXRAMComponentConfig {
    @Expose
    private int m_threadCountMsgHandler = 1;

    @Expose
    private int m_requestMapEntryCount = (int) Math.pow(2, 20);

    @Expose
    private StorageUnit m_incomingBufferSize = new StorageUnit(512, StorageUnit.KB);

    @Expose
    private StorageUnit m_outgoingBufferSize = new StorageUnit(1, StorageUnit.MB);

    @Expose
    private StorageUnit m_maxIncomingBufferSize = new StorageUnit(32, StorageUnit.KB);

    @Expose
    private StorageUnit m_flowControlWindowSize = new StorageUnit(1, StorageUnit.MB);

    @Expose
    private TimeUnit m_requestTimeout = new TimeUnit(333, TimeUnit.MS);

    /**
     * Constructor
     */
    public NetworkComponentConfig() {
        super(NetworkComponent.class, true, true);
    }

    /**
     * Number of threads to spawn for handling incoming and assembled network messages
     */
    public int getThreadCountMsgHandler() {
        return m_threadCountMsgHandler;
    }

    /**
     * Size of the map that stores outstanding requests and maps them to their incoming responses
     */
    public int getRequestMapEntryCount() {
        return m_requestMapEntryCount;
    }

    /**
     * Size of the buffer for incoming network data
     */
    public StorageUnit getIncomingBufferSize() {
        return m_incomingBufferSize;
    }

    /**
     * Size of the buffer for outgoing network data
     */
    public StorageUnit getOutgoingBufferSize() {
        return m_outgoingBufferSize;
    }

    /**
     * Size of the buffer for incoming network data
     */
    public StorageUnit getMaxIncomingBufferSize() {
        return m_maxIncomingBufferSize;
    }

    public StorageUnit getFlowControlWindowSize() {
        return m_flowControlWindowSize;
    }

    /**
     * Amount of time to wait until a request that did not receive a response is considered timed out
     */
    public TimeUnit getRequestTimeout() {
        return m_requestTimeout;
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        // TODO kevin
        return true;
    }
}
