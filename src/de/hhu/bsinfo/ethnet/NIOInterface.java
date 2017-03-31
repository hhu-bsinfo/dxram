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

package de.hhu.bsinfo.ethnet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * NIO interface. Every access to the channels is done here.
 *
 * @author Marc Ewert, marc.ewert@hhu.de, 03.09.2014
 */
class NIOInterface {

    private static final int BUFFER_POOL_SIZE = 25;
    private static final Logger LOGGER = LogManager.getFormatterLogger(NIOInterface.class.getSimpleName());

    // Attributes
    private int m_outgoingBufferSize;
    private int m_maxIncomingBufferSize;

    private ArrayDeque<ByteBuffer> m_buffers;
    private ReentrantLock m_lock;

    private ByteBuffer m_writeBuffer;
    private ByteBuffer m_flowControlBytes;

    /**
     * Creates an instance of NIOInterface
     *
     * @param p_incomingBufferSize
     *     the size of incoming buffer
     * @param p_outgoingBufferSize
     *     the size of outgoing buffer
     * @param p_maxIncomingBufferSize
     *     the maximum number of bytes read at once from channel
     */
    NIOInterface(final int p_incomingBufferSize, final int p_outgoingBufferSize, final int p_maxIncomingBufferSize) {
        m_outgoingBufferSize = p_outgoingBufferSize;
        m_maxIncomingBufferSize = p_maxIncomingBufferSize;

        m_buffers = new ArrayDeque<ByteBuffer>();
        for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
            m_buffers.add(ByteBuffer.allocate(m_maxIncomingBufferSize));
        }
        m_lock = new ReentrantLock(false);

        m_writeBuffer = ByteBuffer.allocateDirect(m_outgoingBufferSize);
        m_flowControlBytes = ByteBuffer.allocateDirect(Integer.BYTES);
    }

    /**
     * Reads the NodeID of the remote node that creates this new connection
     *
     * @param p_channel
     *     the channel of the connection
     * @param p_nioSelector
     *     the NIOSelector
     * @return the NodeID
     * @throws IOException
     *     if the connection could not be created
     */
    static short readRemoteNodeID(final SocketChannel p_channel, final NIOSelector p_nioSelector) throws IOException {
        short ret;
        int bytes;
        int counter = 0;
        ByteBuffer buffer = ByteBuffer.allocate(2);

        while (counter < buffer.capacity()) {
            bytes = p_channel.read(buffer);
            if (bytes == -1) {
                p_channel.keyFor(p_nioSelector.getSelector()).cancel();
                p_channel.close();
                return -1;
            }
            counter += bytes;
        }
        buffer.flip();
        ret = buffer.getShort();

        return ret;
    }

    /**
     * Finishes the connection process for the given connection
     *
     * @param p_connection
     *     the connection
     */
    protected static void connect(final NIOConnection p_connection) {
        try {
            if (p_connection.getOutgoingChannel().isConnectionPending()) {
                if (p_connection.getOutgoingChannel().finishConnect()) {
                    p_connection.connected();
                }
            }
        } catch (final IOException e) { /* ignore */ }
    }

    /**
     * Reads from the given connection
     * m_buffer needs to be synchronized externally
     *
     * @param p_connection
     *     the Connection
     * @return whether reading from channel was successful or not (connection is closed then)
     * @throws IOException
     *     if the data could not be read
     */
    protected boolean read(final NIOConnection p_connection) throws IOException {
        boolean ret = true;
        long readBytes;
        ByteBuffer buffer;
        SocketChannel channel;

        m_lock.lock();
        if (!m_buffers.isEmpty()) {
            buffer = m_buffers.poll();
        } else {
            buffer = ByteBuffer.allocate(m_maxIncomingBufferSize);
        }
        m_lock.unlock();
        while (true) {
            try {
                readBytes = p_connection.getIncomingChannel().read(buffer);
            } catch (final ClosedChannelException e) {
                // Channel is closed -> ignore
                break;
            }
            if (readBytes == -1) {
                // Connection closed
                ret = false;
                break;
            } else if (readBytes == 0 && buffer.position() != 0 || readBytes == m_maxIncomingBufferSize) {
                // There is nothing more to read at the moment
                buffer.limit(buffer.position());
                buffer.rewind();

                p_connection.addIncoming(buffer);

                break;
            }
        }

        return ret;
    }

    /**
     * Writes to the given connection
     *
     * @param p_connection
     *     the connection
     * @return whether all data could be written or data is left
     * @throws IOException
     *     if the data could not be written
     */
    protected boolean write(final NIOConnection p_connection) throws IOException {
        boolean ret = true;
        int bytes;
        int size;
        ByteBuffer buffer;
        ByteBuffer view;
        ByteBuffer slice;
        ByteBuffer buf;

        buffer = p_connection.getOutgoingBytes(m_writeBuffer);
        if (buffer != null) {
            int tries = 0;
            while (buffer.remaining() > 0) {
                try {
                    bytes = p_connection.getOutgoingChannel().write(buffer);
                    if (bytes == 0) {
                        if (++tries == 10) {
                            // Read-buffer on the other side is full. Abort writing and schedule buffer for next write
                            if (buffer == m_writeBuffer) {
                                // Copy buffer to avoid manipulation of scheduled data
                                buf = ByteBuffer.allocateDirect(buffer.remaining() + 1);
                                // Add one additional byte at beginning and set position to 1 to recognize it as a re-write later
                                buf.put((byte) 0);
                                buf.put(buffer);
                                buf.position(1);

                                p_connection.addBuffer(buf);
                            } else {
                                p_connection.addBuffer(buffer);
                            }
                            ret = false;
                            break;
                        }
                    } else {
                        tries = 0;
                    }
                } catch (final ClosedChannelException e) {
                    // Channel is closed -> ignore
                    break;
                }
            }
            // ThroughputStatistic.getInstance().outgoingExtern(writtenBytes - length);
        }
        if (ret) {
            ret = !p_connection.dataLeftToWrite();
        }

        return ret;
    }

    /**
     * Read flow control data
     *
     * @param p_connection
     *     the NIOConnection
     */
    void readFlowControlBytes(final NIOConnection p_connection) throws IOException {
        int readBytes;
        int readAllBytes;

        // This is a flow control byte
        m_flowControlBytes.rewind();
        readAllBytes = 0;
        while (readAllBytes < Integer.BYTES) {
            readBytes = p_connection.getOutgoingChannel().read(m_flowControlBytes);

            if (readBytes == -1) {
                // Channel was closed
                return;
            }

            readAllBytes += readBytes;
        }
        p_connection.handleFlowControlMessage(m_flowControlBytes.getInt(0));
    }

    /**
     * Write flow control data
     *
     * @param p_connection
     *     the NIOConnection
     */
    void writeFlowControlBytes(final NIOConnection p_connection) throws IOException {
        int bytes = 0;
        int tries = 0;

        m_flowControlBytes.rewind();
        m_flowControlBytes.putInt(p_connection.getAndResetConfirmedBytes());
        m_flowControlBytes.rewind();

        while (bytes < Integer.BYTES && ++tries < 1000) {
            // Send flow control bytes over incoming channel as this is unused
            bytes += p_connection.getIncomingChannel().write(m_flowControlBytes);
        }

        if (tries == 1000) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could not send flow control data!");
            // #endif /* LOGGER >= ERROR */
        }
    }

    /**
     * Returns the pooled buffer
     *
     * @param p_byteBuffer
     *     the pooled buffer
     */
    void returnBuffer(final ByteBuffer p_byteBuffer) {
        m_lock.lock();
        if (m_buffers.size() < BUFFER_POOL_SIZE) {
            p_byteBuffer.clear();
            m_buffers.push(p_byteBuffer);
        }
        m_lock.unlock();
    }

}
