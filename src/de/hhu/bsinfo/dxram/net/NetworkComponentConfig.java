package de.hhu.bsinfo.dxram.net;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxnet.core.CoreConfig;
import de.hhu.bsinfo.dxnet.ib.IBConfig;
import de.hhu.bsinfo.dxnet.nio.NIOConfig;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

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

        if ("Ethernet".equals(m_core.getDevice())) {
            if (m_nio.getFlowControlWindow().getBytes() > m_nio.getOugoingRingBufferSize().getBytes()) {
                // #if LOGGER >= ERROR
                LOGGER.error("NIO: OS buffer size must be at least twice the size of flow control window size!");
                // #endif /* LOGGER >= ERROR */
                return false;
            }

            if (m_nio.getFlowControlWindow().getBytes() > Integer.MAX_VALUE) {
                // #if LOGGER >= ERROR
                LOGGER.error("NIO: Flow control window size exceeding 2 GB, not allowed");
                // #endif /* LOGGER >= ERROR */
                return false;
            }
        } else if ("Infiniband".equals(m_core.getDevice())) {
            if (m_ib.getIncomingBufferSize().getBytes() > m_ib.getOugoingRingBufferSize().getBytes()) {
                // #if LOGGER >= ERROR
                LOGGER.error("IB in buffer size must be <= outgoing ring buffer size");
                // #endif /* LOGGER >= ERROR */
                return false;
            }

            if (m_ib.getFlowControlMaxRecvReqs() < m_ib.getMaxConnections()) {
                // #if LOGGER >= WARN
                LOGGER.warn("IB m_ibFlowControlMaxRecvReqs < m_maxConnections: This may result in performance penalties when too many nodes are active");
                // #endif /* LOGGER >= WARN */
            }

            if (m_ib.getMaxRecvReqs() < m_ib.getMaxConnections()) {
                // #if LOGGER >= WARN
                LOGGER.warn("IB m_ibMaxRecvReqs < m_maxConnections: This may result in performance penalties when too many nodes are active");
                // #endif /* LOGGER >= WARN */
            }

            if (m_ib.getFlowControlWindow().getBytes() > Integer.MAX_VALUE) {
                // #if LOGGER >= ERROR
                LOGGER.error("IB: Flow control window size exceeding 2 GB, not allowed");
                // #endif /* LOGGER >= ERROR */
                return false;
            }

            if (m_ib.getIncomingBufferSize().getGBDouble() > 2.0) {
                // #if LOGGER >= ERROR
                LOGGER.error("IB: Exceeding max incoming buffer size of 2GB");
                // #endif /* LOGGER >= ERROR */
                return false;
            }

            if (m_ib.getOugoingRingBufferSize().getGBDouble() > 2.0) {
                // #if LOGGER >= ERROR
                LOGGER.error("IB: Exceeding max outgoing buffer size of 2GB");
                // #endif /* LOGGER >= ERROR */
                return false;
            }
        } else {
            // #if LOGGER >= ERROR
            LOGGER.error("Unknown device %s. Valid options: Ethernet or Infiniband", m_core.getDevice());
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        if (m_core.getRequestMapSize() <= (int) Math.pow(2, 15)) {
            // #if LOGGER >= WARN
            LOGGER.warn("Request map entry count is rather small. Requests might be discarded!");
            // #endif /* LOGGER >= WARN */
            return true;
        }

        return true;
    }
}
