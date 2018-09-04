package de.hhu.bsinfo.dxram.net;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxnet.core.CoreConfig;
import de.hhu.bsinfo.dxnet.ib.IBConfig;
import de.hhu.bsinfo.dxnet.nio.NIOConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the NetworkComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 28.07.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
@DXRAMComponentConfig.Settings(component = NetworkComponent.class, supportsSuperpeer = true, supportsPeer = true)
public class NetworkComponentConfig extends DXRAMComponentConfig {
    /**
     * Get the core configuration values
     */
    @Expose
    private CoreConfig m_coreConfig = new CoreConfig();

    /**
     * Get the NIO specific configuration values
     */
    @Expose
    private NIOConfig m_nioConfig = new NIOConfig();

    /**
     * Get the IB specific configuration values
     */
    @Expose
    private IBConfig m_ibConfig = new IBConfig();

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        if ("Ethernet".equals(m_coreConfig.getDevice())) {
            if (m_nioConfig.getFlowControlWindow().getBytes() > m_nioConfig.getOugoingRingBufferSize().getBytes()) {
                LOGGER.error("NIO: OS buffer size must be at least twice the size of flow control window size!");
                return false;
            }

            if (m_nioConfig.getFlowControlWindow().getBytes() > Integer.MAX_VALUE) {
                LOGGER.error("NIO: Flow control window size exceeding 2 GB, not allowed");
                return false;
            }
        } else if ("Infiniband".equals(m_coreConfig.getDevice())) {
            if (m_ibConfig.getIncomingBufferSize().getBytes() > m_ibConfig.getOugoingRingBufferSize().getBytes()) {
                LOGGER.error("IB in buffer size must be <= outgoing ring buffer size");
                return false;
            }

            if (m_ibConfig.getSharedReceiveQueueSize() < m_ibConfig.getSendQueueSize() * m_ibConfig.getMaxConnections()) {
                LOGGER.warn("IB m_srqSize < m_sqSize * m_maxConnections: This may result in performance " +
                        " penalties when too many nodes are active");
            }

            if (m_ibConfig.getSharedSendCompletionQueueSize() < m_ibConfig.getSendQueueSize() * m_ibConfig.getMaxConnections()) {
                LOGGER.warn("IB m_sharedSCQSize < m_sqSize * m_maxConnections: This may result in performance " +
                        "penalties when too many nodes are active");
            }

            if (m_ibConfig.getSharedReceiveQueueSize() < m_ibConfig.getSharedReceiveCompletionQueueSize()) {
                LOGGER.warn("IB m_srqSize < m_sharedRCQSize: This may result in performance penalties when too " +
                        "many nodes are active");
            }

            if (m_ibConfig.getFlowControlWindow().getBytes() > Integer.MAX_VALUE) {
                LOGGER.error("IB: Flow control window size exceeding 2 GB, not allowed");
                return false;
            }

            if (m_ibConfig.getIncomingBufferSize().getGBDouble() > 2.0) {
                LOGGER.error("IB: Exceeding max incoming buffer size of 2GB");
                return false;
            }

            if (m_ibConfig.getOugoingRingBufferSize().getGBDouble() > 2.0) {
                LOGGER.error("IB: Exceeding max outgoing buffer size of 2GB");
                return false;
            }
        } else {
            LOGGER.error("Unknown device %s. Valid options: Ethernet or Infiniband", m_coreConfig.getDevice());
            return false;
        }

        if (m_coreConfig.getRequestMapSize() <= (int) Math.pow(2, 15)) {
            LOGGER.warn("Request map entry count is rather small. Requests might be discarded!");
            return true;
        }

        return true;
    }
}
