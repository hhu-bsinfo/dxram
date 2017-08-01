package de.hhu.bsinfo.net.ib;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.utils.unit.StorageUnit;
import de.hhu.bsinfo.utils.unit.TimeUnit;

/**
 * Dedicated configuration values for IB
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 28.07.2017
 */
public class IBConfig {
    @Expose
    private int m_maxConnections = 100;

    @Expose
    private TimeUnit m_requestTimeOut = new TimeUnit(100, TimeUnit.MS);

    @Expose
    private StorageUnit m_flowControlWindow = new StorageUnit(512, StorageUnit.KB);

    @Expose
    private StorageUnit m_outgoingRingBufferSize = new StorageUnit(2, StorageUnit.MB);

    @Expose
    private StorageUnit m_incomingBufferSize = new StorageUnit(4, StorageUnit.KB);

    @Expose
    private StorageUnit m_incomingBufferPoolTotalSize = new StorageUnit(100, StorageUnit.MB);

    @Expose
    private int m_maxSendReqs = 10;

    @Expose
    private int m_maxRecvReqs = 200;

    @Expose
    private int m_flowControlMaxRecvReqs = 100;

    @Expose
    private boolean m_enableSignalHandler = false;

    @Expose
    private boolean m_enableDebugThread = false;

    /**
     * Default constructor
     */
    public IBConfig() {

    }

    /**
     * Max number of connections to keep before dismissing existing connections (for new ones)
     */
    public int getMaxConnections() {
        return m_maxConnections;
    }

    /**
     * Amount of time to wait until a request that did not receive a response is considered timed out.
     */
    public TimeUnit getRequestTimeOut() {
        return m_requestTimeOut;
    }

    /**
     * Number of bytes to receive on a flow control message before flow control is considered delayed
     */
    public StorageUnit getFlowControlWindow() {
        return m_flowControlWindow;
    }

    /**
     * Size of the ring buffer for outgoing network data (per connection)
     */
    public StorageUnit getOugoingRingBufferSize() {
        return m_outgoingRingBufferSize;
    }

    /**
     * Size of a single buffer to store incoming data (or slices of data) to
     */
    public StorageUnit getIncomingBufferSize() {
        return m_incomingBufferSize;
    }

    /**
     * Total size of the pool for buffers, each of size incomingBufferSize, to use for incoming data
     */
    public StorageUnit getIncomingBufferPoolTotalSize() {
        return m_incomingBufferPoolTotalSize;
    }

    /**
     * Infiniband send queue size for buffers/data (per connection)
     */
    public int getMaxSendReqs() {
        return m_maxSendReqs;
    }

    /**
     * Infiniband recv queue size for buffers/data (shared across all connections)
     */
    public int getMaxRecvReqs() {
        return m_maxRecvReqs;
    }

    /**
     * Infiniband recv queue size for flow control (shared across all connections)
     */
    public int getFlowControlMaxRecvReqs() {
        return m_flowControlMaxRecvReqs;
    }

    /**
     * Enable a signal handler in the IB subsystem to catch signals and print debug info.
     * If enabled, this overwrites the signal handler of the JVM!
     */
    public boolean getEnableSignalHandler() {
        return m_enableSignalHandler;
    }

    /**
     * Enable a debug thread the prints throughput information periodically (for debugging)
     */
    public boolean getEnableDebugThread() {
        return m_enableDebugThread;
    }
}
