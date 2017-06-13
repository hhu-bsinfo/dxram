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
    private int m_threadCountMsgHandler = 2;

    @Expose
    private int m_requestMapEntryCount = (int) Math.pow(2, 20);

    @Expose
    private StorageUnit m_osBufferSize = new StorageUnit(1, StorageUnit.MB);

    @Expose
    private StorageUnit m_flowControlWindowSize = new StorageUnit(256, StorageUnit.KB);

    @Expose
    private TimeUnit m_requestTimeout = new TimeUnit(333, TimeUnit.MS);


    /**
     * Constructor
     */
    public NetworkComponentConfig() {
        super(NetworkComponent.class, true, true);
    }

    /**
     * Number of threads to spawn for handling incoming and assembled network messages.
     */
    public int getThreadCountMsgHandler() {
        return m_threadCountMsgHandler;
    }

    /**
     * Size of the map that stores outstanding requests and maps them to their incoming responses (index is incremented for messages as well).
     */
    public int getRequestMapEntryCount() {
        return m_requestMapEntryCount;
    }

    /**
     * Size of the buffer for incoming and outgoing network data.
     */
    public StorageUnit getOSBufferSize() {
        return m_osBufferSize;
    }

    /**
     * Maximum number of bytes to be send until acknowledgment is needed.
     */
    public StorageUnit getFlowControlWindowSize() {
        return m_flowControlWindowSize;
    }

    /**
     * Amount of time to wait until a request that did not receive a response is considered timed out.
     */
    public TimeUnit getRequestTimeout() {
        return m_requestTimeout;
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {

        if (m_requestMapEntryCount <= (int) Math.pow(2, 15)) {
            // #if LOGGER >= WARN
            LOGGER.warn("Request map entry count is rather small. Requests might be discarded!");
            // #endif /* LOGGER >= WARN */
            return true;
        }

        if (m_flowControlWindowSize.getBytes() * 2 > m_osBufferSize.getBytes()) {
            // #if LOGGER >= ERROR
            LOGGER.error("OS buffer size must be at least twice the size of flow control window size!");
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        return true;
    }
}
