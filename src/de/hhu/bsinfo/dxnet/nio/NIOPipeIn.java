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
import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.MessageHandlers;
import de.hhu.bsinfo.dxnet.core.AbstractFlowControl;
import de.hhu.bsinfo.dxnet.core.AbstractPipeIn;
import de.hhu.bsinfo.dxnet.core.BufferPool;
import de.hhu.bsinfo.dxnet.core.IncomingBufferQueue;
import de.hhu.bsinfo.dxnet.core.MessageDirectory;
import de.hhu.bsinfo.dxnet.core.MessageHeaderPool;
import de.hhu.bsinfo.dxnet.core.RequestMap;
import de.hhu.bsinfo.utils.stats.StatisticsOperation;
import de.hhu.bsinfo.utils.stats.StatisticsRecorderManager;

/**
 * Created by nothaas on 6/9/17.
 */
class NIOPipeIn extends AbstractPipeIn {
    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractPipeIn.class.getSimpleName());
    private static final StatisticsOperation SOP_READ = StatisticsRecorderManager.getOperation(NIOPipeIn.class, "NIORead");
    private static final StatisticsOperation SOP_WRITE_FLOW_CONTROL = StatisticsRecorderManager.getOperation(NIOPipeIn.class, "NIOWriteFlowControl");

    private SocketChannel m_incomingChannel;
    private final BufferPool m_bufferPool;
    private final IncomingBufferQueue m_incomingBufferQueue;
    private final ByteBuffer m_flowControlBytes;

    private final NIOConnection m_parentConnection;

    NIOPipeIn(final short p_ownNodeId, final short p_destinationNodeId, final MessageHeaderPool p_messageHeaderPool, final AbstractFlowControl p_flowControl,
            final MessageDirectory p_messageDirectory, final RequestMap p_requestMap, final MessageHandlers p_messageHandlers, final BufferPool p_bufferPool,
            final IncomingBufferQueue p_incomingBufferQueue, final NIOConnection p_parentConnection) {
        super(p_ownNodeId, p_destinationNodeId, p_messageHeaderPool, p_flowControl, p_messageDirectory, p_requestMap, p_messageHandlers);

        m_incomingChannel = null;
        m_bufferPool = p_bufferPool;
        m_incomingBufferQueue = p_incomingBufferQueue;
        m_flowControlBytes = ByteBuffer.allocateDirect(Integer.BYTES);

        m_parentConnection = p_parentConnection;
    }

    SocketChannel getChannel() {
        return m_incomingChannel;
    }

    void bindIncomingChannel(final SocketChannel p_channel) {
        m_incomingChannel = p_channel;
    }

    @Override
    public void returnProcessedBuffer(final Object p_directBuffer, final long p_unused) {
        m_bufferPool.returnBuffer((BufferPool.DirectBufferWrapper) p_directBuffer);
    }

    @Override
    public boolean isOpen() {
        return m_incomingChannel != null && m_incomingChannel.isOpen();
    }

    /**
     * Reads from the given connection
     * m_buffer needs to be synchronized externally
     *
     * @return whether reading from channel was successful or not (connection is closed then)
     * @throws IOException
     *         if the data could not be read
     */
    boolean read() throws IOException {
        boolean ret = true;
        long readBytes;
        BufferPool.DirectBufferWrapper directBufferWrapper;
        ByteBuffer buffer;

        directBufferWrapper = m_bufferPool.getBuffer();
        buffer = directBufferWrapper.getBuffer();

        // #ifdef STATISTICS
        // SOP_READ.enter();
        // #endif /* STATISTICS */

        while (true) {
            readBytes = m_incomingChannel.read(buffer);
            if (readBytes == -1) {
                // Connection closed
                ret = false;
                break;
            } else if (readBytes == 0 && buffer.position() != 0 || readBytes >= m_bufferPool.getOSBufferSize() * 0.9) {
                // There is nothing more to read at the moment
                buffer.limit(buffer.position());
                buffer.rewind();

                // #if LOGGER >= TRACE
                LOGGER.trace("Posting receive buffer (limit %d) to connection 0x%X", buffer.limit(), getDestinationNodeID());
                // #endif /* LOGGER >= TRACE */

                // Avoid congestion by not allowing more than m_numberOfBuffers buffers to be cached for reading
                while (!m_incomingBufferQueue.pushBuffer(m_parentConnection, directBufferWrapper, 0, directBufferWrapper.getAddress(), buffer.remaining())) {
                    // #if LOGGER == WARN
                    LOGGER.warn("Network-Selector: IncomingBuffer queue is full!");
                    // #endif /* LOGGER == WARN */

                    //Thread.yield();
                    LockSupport.parkNanos(100);
                }

                break;
            }
        }
        // #ifdef STATISTICS
        // SOP_READ.leave();
        // #endif /* STATISTICS */

        return ret;
    }

    /**
     * Write flow control data
     */
    void writeFlowControlBytes() throws IOException {
        int bytes = 0;
        int tries = 0;

        // #ifdef STATISTICS
        // SOP_WRITE_FLOW_CONTROL.enter();
        // #endif /* STATISTICS */

        m_flowControlBytes.rewind();
        m_flowControlBytes.putInt(getFlowControl().getAndResetFlowControlData());
        m_flowControlBytes.rewind();

        while (bytes < Integer.BYTES && ++tries < 1000) {
            // Send flow control bytes over incoming channel as this is unused
            bytes += m_incomingChannel.write(m_flowControlBytes);
        }

        if (tries == 1000) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could not send flow control data!");
            // #endif /* LOGGER >= ERROR */
        }

        // #ifdef STATISTICS
        // SOP_WRITE_FLOW_CONTROL.leave();
        // #endif /* STATISTICS */
    }
}
