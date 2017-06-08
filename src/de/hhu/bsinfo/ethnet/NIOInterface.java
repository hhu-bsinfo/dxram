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
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * NIO interface. Every access to the channels is done here.
 *
 * @author Marc Ewert, marc.ewert@hhu.de, 03.09.2014
 */
class NIOInterface {

    private static final int LARGE_BUFFER_POOL_SIZE = 16;
    private static final int MEDIUM_BUFFER_POOL_SIZE = 16;
    private static final int SMALL_BUFFER_POOL_SIZE = 5 * 1024;
    private static final int LARGE_BUFFER_POOL_FACTOR = 2;
    private static final int MEDIUM_BUFFER_POOL_FACTOR= 4;
    private static final int SMALL_BUFFER_POOL_FACTOR = 32;
    private static final Logger LOGGER = LogManager.getFormatterLogger(NIOInterface.class.getSimpleName());

    // Attributes
    private int m_osBufferSize;

    private ArrayList<ByteBuffer> m_largeBufferPool;
    private ArrayList<ByteBuffer> m_mediumBufferPool;
    private ArrayList<ByteBuffer> m_smallBufferPool;
    private ReentrantLock m_bufferPoolLock;

    private ByteBuffer m_flowControlBytes;

    /**
     * Creates an instance of NIOInterface
     *
     * @param p_osBufferSize
     *     the size of incoming buffer
     */
    NIOInterface(final int p_osBufferSize) {
        m_osBufferSize = p_osBufferSize;

        m_largeBufferPool = new ArrayList<ByteBuffer>();
        m_mediumBufferPool = new ArrayList<ByteBuffer>();
        m_smallBufferPool = new ArrayList<ByteBuffer>();
        for (int i = 0; i < LARGE_BUFFER_POOL_SIZE; i++) {
            m_largeBufferPool.add(ByteBuffer.allocate(m_osBufferSize / LARGE_BUFFER_POOL_FACTOR));
        }
        for (int i = 0; i < MEDIUM_BUFFER_POOL_SIZE; i++) {
            m_mediumBufferPool.add(ByteBuffer.allocate(m_osBufferSize / MEDIUM_BUFFER_POOL_FACTOR));
        }
        for (int i = 0; i < SMALL_BUFFER_POOL_SIZE; i++) {
            m_smallBufferPool.add(ByteBuffer.allocate(m_osBufferSize / SMALL_BUFFER_POOL_FACTOR));
        }
        m_bufferPoolLock = new ReentrantLock(false);
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
        ByteBuffer buffer = ByteBuffer.allocateDirect(2);

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
     * @param p_key
     *     the selection key
     */
    protected static void connect(final NIOConnection p_connection, final SelectionKey p_key) {
        if (p_connection.getOutgoingChannel().isConnectionPending()) {
            try {
                if (p_connection.getOutgoingChannel().finishConnect()) {
                    p_connection.connected(p_key);
                } else {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Connection could not be finished: %s", p_connection);
                    // #endif /* LOGGER >= ERROR */
                }
            } catch (final IOException ignore) {
                p_connection.abortConnectionCreation();
            }
        } else {
            // #if LOGGER >= WARN
            LOGGER.warn("Connection is not pending, connect aborted: %s", p_connection);
            // #endif /* LOGGER >= WARN */
        }
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

        m_bufferPoolLock.lock();
        if (!m_largeBufferPool.isEmpty()) {
            buffer = m_largeBufferPool.remove(m_largeBufferPool.size() - 1);
        } else if (!m_mediumBufferPool.isEmpty()) {
            buffer = m_mediumBufferPool.remove(m_mediumBufferPool.size() - 1);
        } else if (!m_smallBufferPool.isEmpty()) {
            buffer = m_smallBufferPool.remove(m_smallBufferPool.size() - 1);
        } else {
            // #if LOGGER >= WARN
            LOGGER.warn("Insufficient pooled incoming buffers. Allocating temporary buffer.");
            // #endif /* LOGGER >= WARN */
            buffer = ByteBuffer.allocate(m_osBufferSize);
        }
        m_bufferPoolLock.unlock();

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
            } else if (readBytes == 0 && buffer.position() != 0 || readBytes >= m_osBufferSize * 0.9) {
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
     * @return whether all data could be written
     * @throws IOException
     *     if the data could not be written
     */
    protected static boolean write(final NIOConnection p_connection) throws IOException {
        boolean ret = true;
        int bytes;
        ByteBuffer buffer;

        buffer = p_connection.getOutgoingBytes();
        if (buffer != null) {
            while (buffer.remaining() > 0) {
                try {
                    bytes = p_connection.getOutgoingChannel().write(buffer);
                    if (bytes == 0) {
                        // Read-buffer on the other side is full. Abort writing and schedule buffer for next write
                        p_connection.addBuffer(buffer);
                        ret = false;
                        break;
                    }
                } catch (final ClosedChannelException | NotYetConnectedException e) {
                    // Channel is closed or cannot be opened -> ignore
                    break;
                }
            }
            // ThroughputStatistic.getInstance().outgoingExtern(writtenBytes - length);
            if (ret) {
                p_connection.returnWriteBuffer(buffer);
            }
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
        m_bufferPoolLock.lock();
        p_byteBuffer.clear();
        if (p_byteBuffer.capacity() == m_osBufferSize / LARGE_BUFFER_POOL_FACTOR) {
            if (m_largeBufferPool.size() < LARGE_BUFFER_POOL_SIZE) {
                m_largeBufferPool.add(p_byteBuffer);
            }
        } else if (p_byteBuffer.capacity() == m_osBufferSize / MEDIUM_BUFFER_POOL_FACTOR) {
            if (m_mediumBufferPool.size() < MEDIUM_BUFFER_POOL_SIZE) {
                m_mediumBufferPool.add(p_byteBuffer);
            }
        } else  if (p_byteBuffer.capacity() == m_osBufferSize / SMALL_BUFFER_POOL_FACTOR) {
            if (m_smallBufferPool.size() < SMALL_BUFFER_POOL_SIZE) {
                m_smallBufferPool.add(p_byteBuffer);
            }
        }
        m_bufferPoolLock.unlock();
    }

}