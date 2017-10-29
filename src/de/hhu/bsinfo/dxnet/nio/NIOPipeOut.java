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

package de.hhu.bsinfo.dxnet.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.NodeMap;
import de.hhu.bsinfo.dxnet.core.AbstractFlowControl;
import de.hhu.bsinfo.dxnet.core.AbstractPipeOut;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxnet.core.OutgoingRingBuffer;
import de.hhu.bsinfo.utils.stats.StatisticsOperation;
import de.hhu.bsinfo.utils.stats.StatisticsRecorderManager;

/**
 * Created by nothaas on 6/9/17.
 */
public class NIOPipeOut extends AbstractPipeOut {
    private static final Logger LOGGER = LogManager.getFormatterLogger(NIOPipeOut.class.getSimpleName());
    private static final StatisticsOperation SOP_WRITE = StatisticsRecorderManager.getOperation(NIOPipeOut.class, "NIOWrite");
    private static final StatisticsOperation SOP_READ_FLOW_CONTROL = StatisticsRecorderManager.getOperation(NIOPipeOut.class, "NIOReadFlowControl");

    private final int m_bufferSize;

    private SocketChannel m_outgoingChannel;
    private final ChangeOperationsRequest m_writeOperation;

    private final NIOSelector m_nioSelector;
    private final NodeMap m_nodeMap;
    private final ByteBuffer m_flowControlBytes;

    NIOPipeOut(final short p_ownNodeId, final short p_destinationNodeId, final int p_bufferSize, final AbstractFlowControl p_flowControl,
            final OutgoingRingBuffer p_outgoingBuffer, final NIOSelector p_nioSelector, final NodeMap p_nodeMap, final NIOConnection p_parentConnection) {
        super(p_ownNodeId, p_destinationNodeId, p_flowControl, p_outgoingBuffer);

        m_bufferSize = p_bufferSize;

        m_outgoingChannel = null;
        m_writeOperation = new ChangeOperationsRequest(p_parentConnection, NIOSelector.WRITE);

        m_nioSelector = p_nioSelector;
        m_nodeMap = p_nodeMap;
        m_flowControlBytes = ByteBuffer.allocateDirect(Integer.BYTES);
    }

    SocketChannel getChannel() {
        return m_outgoingChannel;
    }

    void createOutgoingChannel(final short p_nodeID) throws NetworkException {
        try {
            m_outgoingChannel = SocketChannel.open();
            m_outgoingChannel.configureBlocking(false);
            m_outgoingChannel.socket().setSoTimeout(0);
            m_outgoingChannel.socket().setTcpNoDelay(true);
            m_outgoingChannel.socket().setReceiveBufferSize(32);
            m_outgoingChannel.socket().setSendBufferSize(m_bufferSize);
            int sendBufferSize = m_outgoingChannel.socket().getSendBufferSize();
            if (sendBufferSize < m_bufferSize) {
                // #if LOGGER >= WARN
                LOGGER.warn("Send buffer size could not be set properly. Check OS settings! Requested: %d, actual: %d", m_bufferSize, sendBufferSize);
                // #endif /* LOGGER >= WARN */
            }

            m_outgoingChannel.connect(m_nodeMap.getAddress(p_nodeID));
        } catch (final IOException ignored) {
            throw new NetworkException("Creating outgoing channel failed");
        }
    }

    // for NIO (initial message sending node id)
    void postBuffer(final ByteBuffer p_buffer) throws NetworkException {
        ((NIOOutgoingRingBuffer) getOutgoingQueue()).pushNodeID(p_buffer);
        bufferPosted(p_buffer.remaining());
    }

    /**
     * Writes to the given connection
     *
     * @return whether all data could be written
     * @throws IOException
     *         if the data could not be written
     */
    boolean write() throws IOException {
        boolean ret = true;
        int writtenBytes = 0;
        int bytes;
        ByteBuffer buffer;

        // #ifdef STATISTICS
        SOP_WRITE.enter();
        // #endif /* STATISTICS */

        buffer = ((NIOOutgoingRingBuffer) getOutgoingQueue()).popFront();
        if (buffer != null && buffer.position() != buffer.limit()) {
            while (buffer.remaining() > 0) {

                bytes = m_outgoingChannel.write(buffer);
                if (bytes == 0) {
                    // Read-buffer on the other side is full. Abort writing and schedule buffer for next write
                    ret = false;
                    break;
                }
                writtenBytes += bytes;
            }
            getOutgoingQueue().shiftBack(writtenBytes);
        }

        // #ifdef STATISTICS
        SOP_WRITE.leave();
        // #endif /* STATISTICS */

        return ret;
    }

    /**
     * Read flow control data
     */
    void readFlowControlBytes() throws IOException {
        int readBytes;
        int readAllBytes;

        // #ifdef STATISTICS
        SOP_READ_FLOW_CONTROL.enter();
        // #endif /* STATISTICS */

        // This is a flow control byte
        m_flowControlBytes.rewind();
        readAllBytes = 0;
        while (readAllBytes < Integer.BYTES) {
            readBytes = m_outgoingChannel.read(m_flowControlBytes);

            if (readBytes == -1) {
                // Channel was closed
                return;
            }

            readAllBytes += readBytes;
        }

        getFlowControl().handleFlowControlData(m_flowControlBytes.getInt(0));

        // #ifdef STATISTICS
        SOP_READ_FLOW_CONTROL.leave();
        // #endif /* STATISTICS */
    }

    @Override
    protected boolean isOpen() {
        return m_outgoingChannel != null && m_outgoingChannel.isOpen();
    }

    @Override
    protected void bufferPosted(final int p_size) {
        // Change operation (read <-> write) and/or connection
        m_nioSelector.changeOperationInterestAsync(m_writeOperation);
    }
}
