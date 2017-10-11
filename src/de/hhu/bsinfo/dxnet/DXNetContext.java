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

package de.hhu.bsinfo.dxnet;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.core.CoreConfig;
import de.hhu.bsinfo.dxnet.ib.IBConfig;
import de.hhu.bsinfo.dxnet.loopback.LoopbackConfig;
import de.hhu.bsinfo.dxnet.nio.NIOConfig;

/**
 * Context object with settings for DXNet
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 21.09.2017
 */
public class DXNetContext {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXNetContext.class.getSimpleName());

    /**
     * DXNet specific settings
     */
    @Expose
    private CoreConfig m_coreConfig = new CoreConfig();

    @Expose
    private NIOConfig m_nioConfig = new NIOConfig();

    @Expose
    private IBConfig m_ibConfig = new IBConfig();

    @Expose
    private LoopbackConfig m_loopbackConfig = new LoopbackConfig();

    /**
     * Constructor
     */
    DXNetContext() {

    }

    /**
     * Get the core configuration
     *
     * @return Configuration
     */
    CoreConfig getCoreConfig() {
        return m_coreConfig;
    }

    /**
     * Get the nio configuration
     *
     * @return Configuration
     */
    NIOConfig getNIOConfig() {
        return m_nioConfig;
    }

    /**
     * Get the ib configuration
     *
     * @return Configuration
     */
    IBConfig getIBConfig() {
        return m_ibConfig;
    }

    /**
     * Get the loopback configuration
     *
     * @return Configuration
     */
    LoopbackConfig getLoopbackConfig() {
        return m_loopbackConfig;
    }

    /**
     * @return
     */
    protected boolean verify() {
        if (m_coreConfig.getRequestMapSize() <= (int) Math.pow(2, 15)) {
            // #if LOGGER >= WARN
            LOGGER.warn("Request map entry count is rather small. Requests might be discarded!");
            // #endif /* LOGGER >= WARN */
            return true;
        }

        if (m_nioConfig.getFlowControlWindow().getBytes() * 2 > m_nioConfig.getOugoingRingBufferSize().getBytes()) {
            // #if LOGGER >= ERROR
            LOGGER.error("NIO: OS buffer size must be at least twice the size of flow control window size!");
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        if (m_ibConfig.getIncomingBufferSize().getBytes() > m_ibConfig.getOugoingRingBufferSize().getBytes()) {
            // #if LOGGER >= ERROR
            LOGGER.error("IB in buffer size must be <= outgoing ring buffer size");
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        if (m_ibConfig.getFlowControlMaxRecvReqs() < m_ibConfig.getMaxConnections()) {
            // #if LOGGER >= WARN
            LOGGER.warn("IB m_ibFlowControlMaxRecvReqs < m_maxConnections: This may result in performance penalties when too many nodes are active");
            // #endif /* LOGGER >= WARN */
        }

        if (m_ibConfig.getMaxRecvReqs() < m_ibConfig.getMaxConnections()) {
            // #if LOGGER >= WARN
            LOGGER.warn("IB m_ibMaxRecvReqs < m_maxConnections: This may result in performance penalties when too many nodes are active");
            // #endif /* LOGGER >= WARN */
        }

        if (m_nioConfig.getFlowControlWindow().getBytes() > Integer.MAX_VALUE) {
            // #if LOGGER >= ERROR
            LOGGER.error("NIO: Flow control window size exceeding 2 GB, not allowed");
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        if (m_ibConfig.getFlowControlWindow().getBytes() > Integer.MAX_VALUE) {
            // #if LOGGER >= ERROR
            LOGGER.error("IB: Flow control window size exceeding 2 GB, not allowed");
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        if (m_ibConfig.getIncomingBufferSize().getGBDouble() > 2.0) {
            // #if LOGGER >= ERROR
            LOGGER.error("IB: Exceeding max incoming buffer size of 2GB");
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        if (m_ibConfig.getOugoingRingBufferSize().getGBDouble() > 2.0) {
            // #if LOGGER >= ERROR
            LOGGER.error("IB: Exceeding max outgoing buffer size of 2GB");
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        return true;
    }

}
