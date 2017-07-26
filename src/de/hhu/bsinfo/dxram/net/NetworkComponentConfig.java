package de.hhu.bsinfo.dxram.net;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.utils.unit.StorageUnit;
import de.hhu.bsinfo.utils.unit.TimeUnit;

/**
 * Config for the NetworkComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class NetworkComponentConfig extends AbstractDXRAMComponentConfig {
    @Expose
    private int m_maxConnections = 1000;

    @Expose
    private StorageUnit m_bufferSize = new StorageUnit(1, StorageUnit.MB);

    @Expose
    private StorageUnit m_flowControlWindowSize = new StorageUnit(256, StorageUnit.KB);

    @Expose
    private int m_numMessageHandlerThreads = 2;

    @Expose
    private int m_requestMapEntryCount = (int) Math.pow(2, 20);

    @Expose
    private TimeUnit m_requestTimeout = new TimeUnit(333, TimeUnit.MS);

    @Expose
    private TimeUnit m_connectionTimeout = new TimeUnit(333, TimeUnit.MS);

    @Expose
    private boolean m_useStaticExporterPool = true;

    @Expose
    private boolean m_infiniband = false;

    @Expose
    private int m_ibBufferPoolSize = 100;

    @Expose
    private int m_ibMaxRecvReqs = 10;

    @Expose
    private int m_ibMaxSendReqs = 10;

    @Expose
    private int m_ibFlowControlMaxRecvReqs = 10;

    @Expose
    private int m_ibFlowControlMaxSendReqs = 10;

    @Expose
    private int m_ibOutgoingJobPoolSize = 1000;

    @Expose
    private int m_ibSendThreads = 1;

    @Expose
    private int m_ibRecvThreads = 1;

    @Expose
    private boolean m_ibEnableSignalHandler = false;

    @Expose
    private boolean m_ibEnableDebugThread = false;

    /**
     * Constructor
     */
    public NetworkComponentConfig() {
        super(NetworkComponent.class, true, true);
    }

    /**
     * Max number of connections to keep before dismissing existing connections (for new ones)
     */
    public int getMaxConnections() {
        return m_maxConnections;
    }

    /**
     * Size of the buffer for incoming and outgoing network data
     */
    public StorageUnit getBufferSize() {
        return m_bufferSize;
    }

    /**
     * Number of bytes to receive on a flow control message before flow control is considered delayed
     */
    public StorageUnit getFlowControlWindowSize() {
        return m_flowControlWindowSize;
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
    public int getRequestMapEntryCount() {
        return m_requestMapEntryCount;
    }

    /**
     * Amount of time to wait until a request that did not receive a response is considered timed out.
     */
    public TimeUnit getRequestTimeout() {
        return m_requestTimeout;
    }

    /**
     * Amount of time to try to establish a connection before giving up
     */
    public TimeUnit getConnectionTimeout() {
        return m_connectionTimeout;
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
    public boolean isInfiniband() {
        return m_infiniband;
    }

    /**
     * Initial pool size for DirectBuffers used to receive data. Each buffer has the specified buffer size
     */
    public int getIbBufferPoolSize() {
        return m_ibBufferPoolSize;
    }

    /**
     * Infiniband recv queue size for buffers/data (shared across all connections)
     */
    public int getIbMaxRecvReqs() {
        return m_ibMaxRecvReqs;
    }

    /**
     * Infiniband send queue size for buffers/data (per connection)
     */
    public int getIbMaxSendReqs() {
        return m_ibMaxSendReqs;
    }

    /**
     * Infiniband send queue size for flow control (per connection)
     */
    public int getIbFlowControlMaxSendReqs() {
        return m_ibFlowControlMaxSendReqs;
    }

    /**
     * Infiniband recv queue size for flow control (shared across all connections)
     */
    public int getIbFlowControlMaxRecvReqs() {
        return m_ibFlowControlMaxRecvReqs;
    }

    /**
     * Size of the job pool that separates the application threads from the
     * Infiniband subsystem send thread
     */
    public int getIbOutgoingJobPoolSize() {
        return m_ibOutgoingJobPoolSize;
    }

    /**
     * Number of send threads to use inside the IB subsystem to put data to the IB queues
     */
    public int getIbSendThreads() {
        return m_ibSendThreads;
    }

    /**
     * Number of recv threads to use to grab data from the incoming queue in the IB subsystem
     */
    public int getIbRecvThreads() {
        return m_ibRecvThreads;
    }

    /**
     * Enable a signal handler in the IB subsystem to catch signals and print debug info.
     * If enabled, this overwrites the signal handler of the JVM!
     */
    public boolean getIbEnableSignalHandler() {
        return m_ibEnableSignalHandler;
    }

    /**
     * Enable a debug thread the prints throughput information periodically (for debugging)
     */
    public boolean getIbEnableDebugThread() {
        return m_ibEnableDebugThread;
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {

        if (m_requestMapEntryCount <= (int) Math.pow(2, 15)) {
            // #if LOGGER >= WARN
            LOGGER.warn("Request map entry count is rather small. Requests might be discarded!");
            // #endif /* LOGGER >= WARN */
            return true;
        }

        if (m_flowControlWindowSize.getBytes() * 2 > m_bufferSize.getBytes()) {
            // #if LOGGER >= ERROR
            LOGGER.error("OS buffer size must be at least twice the size of flow control window size!");
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        return true;
    }
}
