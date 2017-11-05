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

package de.hhu.bsinfo.dxnet.ib;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.MessageHandlers;
import de.hhu.bsinfo.dxnet.NetworkDestinationUnreachableException;
import de.hhu.bsinfo.dxnet.NodeMap;
import de.hhu.bsinfo.dxnet.core.AbstractConnection;
import de.hhu.bsinfo.dxnet.core.AbstractConnectionManager;
import de.hhu.bsinfo.dxnet.core.AbstractExporterPool;
import de.hhu.bsinfo.dxnet.core.CoreConfig;
import de.hhu.bsinfo.dxnet.core.DynamicExporterPool;
import de.hhu.bsinfo.dxnet.core.IncomingBufferQueue;
import de.hhu.bsinfo.dxnet.core.MessageDirectory;
import de.hhu.bsinfo.dxnet.core.MessageHeaderPool;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxnet.core.NetworkRuntimeException;
import de.hhu.bsinfo.dxnet.core.RequestMap;
import de.hhu.bsinfo.dxnet.core.StaticExporterPool;
import de.hhu.bsinfo.utils.ByteBufferHelper;
import de.hhu.bsinfo.utils.NodeID;
import de.hhu.bsinfo.utils.stats.StatisticsOperation;
import de.hhu.bsinfo.utils.stats.StatisticsRecorderManager;

/**
 * Connection manager for infiniband (note: this is the main class for the IB subsystem in the java space)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 13.06.2017
 */
public class IBConnectionManager extends AbstractConnectionManager implements JNIIbdxnet.SendHandler, JNIIbdxnet.RecvHandler, JNIIbdxnet.ConnectionHandler {
    private static final Logger LOGGER = LogManager.getFormatterLogger(IBConnectionManager.class.getSimpleName());

    private static final StatisticsOperation SOP_SEND_NEXT_DATA = StatisticsRecorderManager.getOperation(IBConnectionManager.class, "SendNextData");

    private final CoreConfig m_coreConfig;
    private final IBConfig m_config;
    private final NodeMap m_nodeMap;

    private final MessageDirectory m_messageDirectory;
    private final RequestMap m_requestMap;
    private final IncomingBufferQueue m_incomingBufferQueue;
    private final MessageHeaderPool m_messageHeaderPool;
    private final MessageHandlers m_messageHandlers;

    private AbstractExporterPool m_exporterPool;

    private final IBWriteInterestManager m_writeInterestManager;

    private final boolean[] m_nodeConnected;

    //    struct NextWorkParameters
    //    {
    //        uint32_t m_posFrontRel;
    //        uint32_t m_posBackRel;
    //        uint32_t m_flowControlData;
    //        uint16_t m_nodeId;
    //    } __attribute__((packed));
    private final ByteBuffer m_sendThreadRetArgs;

    /**
     * Constructor
     *
     * @param p_coreConfig
     *         Core configuration instance with core config values
     * @param p_config
     *         IB configuration instance with IB specific config values
     * @param p_nodeMap
     *         Node map instance
     * @param p_messageDirectory
     *         Message directory instance
     * @param p_requestMap
     *         Request map instance
     * @param p_incomingBufferQueue
     *         Incoming buffer queue instance
     * @param p_messageHandlers
     *         Message handlers instance
     */
    public IBConnectionManager(final CoreConfig p_coreConfig, final IBConfig p_config, final NodeMap p_nodeMap, final MessageDirectory p_messageDirectory,
            final RequestMap p_requestMap, final IncomingBufferQueue p_incomingBufferQueue, final MessageHeaderPool p_messageHeaderPool,
            final MessageHandlers p_messageHandlers, final boolean p_overprovisioning) {
        super(p_config.getMaxConnections(), p_overprovisioning);

        m_coreConfig = p_coreConfig;
        m_config = p_config;
        m_nodeMap = p_nodeMap;

        m_messageDirectory = p_messageDirectory;
        m_requestMap = p_requestMap;
        m_incomingBufferQueue = p_incomingBufferQueue;
        m_messageHeaderPool = p_messageHeaderPool;
        m_messageHandlers = p_messageHandlers;

        if (p_coreConfig.getExporterPoolType()) {
            m_exporterPool = new StaticExporterPool();
        } else {
            m_exporterPool = new DynamicExporterPool();
        }

        m_writeInterestManager = new IBWriteInterestManager();

        m_nodeConnected = new boolean[NodeID.MAX_ID];

        m_sendThreadRetArgs = ByteBuffer.allocateDirect(Long.BYTES + Integer.BYTES + Integer.BYTES + Short.BYTES);
        // consider native byte order (most likely little endian)
        m_sendThreadRetArgs.order(ByteOrder.nativeOrder());
    }

    /**
     * Initialize the infiniband subsystem. This calls to the underlying Ibdxnet subsystem and requires the respective library to be loaded
     */
    public void init() {
        // can't call this in the constructor because it relies on the implemented interfaces for callbacks
        if (!JNIIbdxnet
                .init(m_coreConfig.getOwnNodeId(), (int) m_config.getIncomingBufferSize().getBytes(), (int) m_config.getOugoingRingBufferSize().getBytes(),
                        m_config.getIncomingBufferPoolTotalSize().getBytes(), m_config.getMaxRecvReqs(), m_config.getMaxSendReqs(),
                        m_config.getFlowControlMaxRecvReqs(), m_config.getMaxConnections(), (int) m_config.getConnectionCreationTimeout().getMs(), this, this,
                        this, m_config.getEnableSignalHandler(), m_config.getEnableDebugThread())) {
            // #if LOGGER >= DEBUG
            LOGGER.debug("Initializing ibnet failed, check ibnet logs");
            // #endif /* LOGGER >= DEBUG */

            throw new NetworkRuntimeException("Initializing ibnet failed");
        }

        // this is an ugly way of figuring out which nodes are available on startup. the ib subsystem needs that kind of information to
        // contact the nodes using an ethernet connection to exchange ib connection information
        // if you know a better/faster way of doing this here, be my guest and fix it
        for (int i = 0; i < NodeID.MAX_ID; i++) {
            if (i == (m_coreConfig.getOwnNodeId() & 0xFFFF)) {
                continue;
            }

            InetSocketAddress addr = m_nodeMap.getAddress((short) i);

            if (!"/255.255.255.255".equals(addr.getAddress().toString())) {
                byte[] bytes = addr.getAddress().getAddress();
                int val = (int) (((long) bytes[0] & 0xFF) << 24 | ((long) bytes[1] & 0xFF) << 16 | ((long) bytes[2] & 0xFF) << 8 | bytes[3] & 0xFF);
                JNIIbdxnet.addNode(val);
            }
        }
    }

    @Override
    public void close() {
        // #if LOGGER >= DEBUG
        LOGGER.debug("Closing connection manager");
        // #endif /* LOGGER >= DEBUG */

        super.close();

        JNIIbdxnet.shutdown();
    }

    @Override
    protected AbstractConnection createConnection(final short p_destination, final AbstractConnection p_existingConnection) throws NetworkException {
        IBConnection connection;

        if (!m_nodeConnected[p_destination & 0xFFFF]) {
            throw new NetworkDestinationUnreachableException(p_destination);
        }

        m_connectionCreationLock.lock();
        // FIXME see fixme on AbstractConnectionManager
        if (m_openConnections == m_config.getMaxConnections()) {
            // dismissRandomConnection();
        }

        connection = (IBConnection) m_connections[p_destination & 0xFFFF];

        if (connection == null) {
            connection = new IBConnection(m_coreConfig.getOwnNodeId(), p_destination, (int) m_config.getOugoingRingBufferSize().getBytes(),
                    (int) m_config.getFlowControlWindow().getBytes(), m_config.getFlowControlWindowThreshold(), m_messageHeaderPool, m_messageDirectory,
                    m_requestMap, m_exporterPool, m_messageHandlers, m_writeInterestManager);
            m_openConnections++;
        }

        m_connectionCreationLock.unlock();

        connection.setPipeInConnected(true);
        connection.setPipeOutConnected(true);

        return connection;
    }

    @Override
    protected void closeConnection(final AbstractConnection p_connection, final boolean p_removeConnection) {
        // #if LOGGER >= DEBUG
        LOGGER.debug("Closing connection 0x%X", p_connection.getDestinationNodeID());
        // #endif /* LOGGER >= DEBUG */

        p_connection.setPipeInConnected(false);
        p_connection.setPipeOutConnected(false);

        m_connectionCreationLock.lock();
        AbstractConnection tmp = m_connections[p_connection.getDestinationNodeID() & 0xFFFF];
        if (p_connection.equals(tmp)) {
            p_connection.close(p_removeConnection);
            m_connections[p_connection.getDestinationNodeID() & 0xFFFF] = null;
            m_openConnections--;
        }
        m_connectionCreationLock.unlock();

        // Trigger failure handling for remote node over faulty connection
        m_listener.connectionLost(p_connection.getDestinationNodeID());
    }

    @Override
    public void nodeDiscovered(final short p_nodeId) {
        // #if LOGGER >= DEBUG
        LOGGER.debug("Node discovered 0x%X", p_nodeId);
        // #endif /* LOGGER >= DEBUG */

        m_nodeConnected[p_nodeId & 0xFFFF] = true;
    }

    @Override
    public void nodeInvalidated(final short p_nodeId) {
        // #if LOGGER >= DEBUG
        LOGGER.debug("Node invalidated 0x%X", p_nodeId);
        // #endif /* LOGGER >= DEBUG */

        m_nodeConnected[p_nodeId & 0xFFFF] = false;
    }

    @Override
    public void nodeConnected(final short p_nodeId) {
        // #if LOGGER >= DEBUG
        LOGGER.debug("Node connected 0x%X", p_nodeId);
        // #endif /* LOGGER >= DEBUG */
    }

    @Override
    public void nodeDisconnected(final short p_nodeId) {
        // #if LOGGER >= DEBUG
        LOGGER.debug("Node disconnected 0x%X", p_nodeId);
        // #endif /* LOGGER >= DEBUG */

        closeConnection(m_connections[p_nodeId], true);
    }

    @Override
    public long getNextDataToSend(final short p_prevNodeIdWritten, final int p_prevDataWrittenLen) {
        // return interest of previous call
        if (p_prevNodeIdWritten != NodeID.INVALID_ID) {
            // #ifdef STATISTICS
            SOP_SEND_NEXT_DATA.leave();
            // #endif /* STATISTICS */

            // #if LOGGER >= TRACE
            LOGGER.trace("getNextDataToSend, p_prevNodeIdWritten 0x%X, p_prevDataWrittenLen %d", p_prevNodeIdWritten, p_prevDataWrittenLen);
            // #endif /* LOGGER >= TRACE */

            // also notify that previous data has been processed (if connection is still available)
            try {
                IBConnection prevConnection = (IBConnection) getConnection(p_prevNodeIdWritten);
                prevConnection.getPipeOut().dataProcessed(p_prevDataWrittenLen);
            } catch (final NetworkException e) {
                // #if LOGGER >= ERROR
                LOGGER.trace("Getting connection 0x%X for previous data written failed", p_prevNodeIdWritten);
                // #endif /* LOGGER >= ERROR */
            }
        }

        // poll for next interest
        short nodeId = m_writeInterestManager.getNextInterests();

        // no data available
        if (nodeId == NodeID.INVALID_ID) {
            return 0;
        }

        // #if LOGGER >= TRACE
        LOGGER.trace("Next write interests on node 0x%X", nodeId);
        // #endif /* LOGGER >= TRACE */

        // prepare next work load
        IBConnection connection;
        try {
            connection = (IBConnection) getConnection(nodeId);
        } catch (final NetworkException ignored) {
            m_writeInterestManager.nodeDisconnected(nodeId);
            return 0;
        }

        // assemble arguments for struct to pass back to jni
        m_sendThreadRetArgs.clear();

        long interests = m_writeInterestManager.consumeInterests(nodeId);

        // process data interests
        if ((int) interests > 0) {
            long pos = connection.getPipeOut().getNextBuffer();
            int relPosBackRel = (int) (pos >> 32 & 0x7FFFFFFF);
            int relPosFrontRel = (int) (pos & 0x7FFFFFFF);

            // relative position of data start in buffer
            m_sendThreadRetArgs.putInt(relPosFrontRel);
            // relative position of data end in buffer
            m_sendThreadRetArgs.putInt(relPosBackRel);

            // #if LOGGER >= TRACE
            LOGGER.trace("Next data write on node 0x%X, posFrontRelative %d, posBackRelative %d", nodeId, relPosFrontRel, relPosBackRel);
            // #endif /* LOGGER >= TRACE */

            // check if outgoing is empty or if we got the first part of a wrap around
            // if wrap around -> push back a new interest to not forget the wrap around
            if (!connection.getPipeOut().isOutgoingQueueEmpty()) {
                m_writeInterestManager.pushBackDataInterest(nodeId);
            }
        } else {
            // no data to write, fc only
            m_sendThreadRetArgs.putInt(0);
            m_sendThreadRetArgs.putInt(0);
        }

        // process flow control interests
        if (interests >> 32 > 0) {
            int fcData = connection.getPipeOut().getFlowControlToWrite();

            m_sendThreadRetArgs.putInt(fcData);

            // #if LOGGER >= TRACE
            LOGGER.trace("Next flow control write on node 0x%X, fc data %d", nodeId, fcData);
            // #endif /* LOGGER >= TRACE */
        } else {
            // data only, no fc
            m_sendThreadRetArgs.putInt(0);
        }

        // node id
        m_sendThreadRetArgs.putShort(nodeId);

        // #ifdef STATISTICS
        SOP_SEND_NEXT_DATA.enter();
        // #endif /* STATISTICS */

        return ByteBufferHelper.getDirectAddress(m_sendThreadRetArgs);
    }

    @Override
    public void receivedBuffer(final short p_sourceNodeId, final long p_bufferHandle, final long p_addr, final int p_length) {
        // #if LOGGER >= TRACE
        LOGGER.trace("Received buffer (0x%X, %d) from 0x%X", p_addr, p_length, p_sourceNodeId);
        // #endif /* LOGGER >= TRACE */

        IBConnection connection;
        try {
            connection = (IBConnection) getConnection(p_sourceNodeId);
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Getting connection for recv buffer of node 0x%X failed", p_sourceNodeId, e);
            // #endif /* LOGGER >= ERROR */
            return;
        }

        // Avoid congestion by not allowing more than m_numberOfBuffers buffers to be cached for reading
        while (!m_incomingBufferQueue.pushBuffer(connection, null, p_bufferHandle, p_addr, p_length)) {
            // #if LOGGER == TRACE
            LOGGER.trace("Message creator: IncomingBuffer queue is full!");
            // #endif /* LOGGER == TRACE */

            //Thread.yield();
            LockSupport.parkNanos(100);
        }
    }

    @Override
    public void receivedFlowControlData(final short p_sourceNodeId, final int p_bytes) {
        // #if LOGGER >= TRACE
        LOGGER.trace("Received flow control data (%d) from 0x%X", p_bytes, p_sourceNodeId);
        // #endif /* LOGGER >= TRACE */

        IBConnection connection;
        try {
            connection = (IBConnection) getConnection(p_sourceNodeId);
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Getting connection for recv flow control data of node 0x%X failed", p_sourceNodeId, e);
            // #endif /* LOGGER >= ERROR */
            return;
        }

        connection.getPipeIn().handleFlowControlData(p_bytes);
    }
}
