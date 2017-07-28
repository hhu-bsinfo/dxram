package de.hhu.bsinfo.dxram.net;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.net.core.CoreConfig;
import de.hhu.bsinfo.net.ib.IBConfig;
import de.hhu.bsinfo.net.nio.NIOConfig;

/**
 * Config for the NetworkComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 28.07.2017
 */
public class NetworkComponentConfig extends AbstractDXRAMComponentConfig {
    @Expose
    private CoreConfig m_core = new CoreConfig();

    @Expose
    private NIOConfig m_nio = new NIOConfig();

    @Expose
    private IBConfig m_ib = new IBConfig();

    /**
     * Constructor
     */
    public NetworkComponentConfig() {
        super(NetworkComponent.class, true, true);
    }

    /**
     * Get the core configuration values
     */
    public CoreConfig getCoreConfig() {
        return m_core;
    }

    /**
     * Get the NIO specific configuration values
     */
    public NIOConfig getNIOConfig() {
        return m_nio;
    }

    /**
     * Get the IB specific configuration values
     */
    public IBConfig getIBConfig() {
        return m_ib;
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {

        if (m_core.getRequestMapSize() <= (int) Math.pow(2, 15)) {
            // #if LOGGER >= WARN
            LOGGER.warn("Request map entry count is rather small. Requests might be discarded!");
            // #endif /* LOGGER >= WARN */
            return true;
        }

        if (m_nio.getFlowControlWindow().getBytes() * 2 > m_nio.getOugoingRingBufferSize().getBytes()) {
            // #if LOGGER >= ERROR
            LOGGER.error("NIO: OS buffer size must be at least twice the size of flow control window size!");
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        if (m_ib.getIncomingBufferSize().getBytes() <= m_ib.getOugoingRingBufferSize().getBytes()) {
            // #if LOGGER >= ERROR
            LOGGER.error("IB in buffer size must be <= outgoing ring buffer size");
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        if (m_ib.getFlowControlMaxRecvReqs() < m_ib.getMaxConnections()) {
            // #if LOGGER >= WARN
            LOGGER.warn("IB m_ibFlowControlMaxRecvReqs < m_maxConnections: This may result in performance penalties when too many nodes are active");
            // #endif /* LOGGER >= WARN */
            return false;
        }

        if (m_ib.getMaxRecvReqs() < m_ib.getMaxConnections()) {
            // #if LOGGER >= WARN
            LOGGER.warn("IB m_ibMaxRecvReqs < m_maxConnections: This may result in performance penalties when too many nodes are active");
            // #endif /* LOGGER >= WARN */
            return false;
        }

        return true;
    }
}
