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
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Represents a network connection
 *
 * @author Florian Klein, florian.klein@hhu.de, 18.03.2012
 */
class NIOConnection extends AbstractConnection {

    private static final Logger LOGGER = LogManager.getFormatterLogger(NIOConnection.class.getSimpleName());

    // Attributes
    private SocketChannel m_outgoingChannel;
    private SocketChannel m_incomingChannel;
    private NIOSelector m_nioSelector;
    private MessageCreator m_messageCreator;
    private int m_osBufferSize;
    private OutgoingQueue m_outgoing;
    private ReentrantLock m_connectionCondLock;
    private Condition m_connectionCond;

    private ChangeOperationsRequest m_writeOperation;
    private ChangeOperationsRequest m_flowControlOperation;

    private ReentrantLock m_sliceLock;

    private volatile boolean m_aborted;

    // Constructors

    /**
     * Creates an instance of NIOConnection (this node creates a new connection with another node)
     *
     * @param p_destination
     *         the destination
     * @param p_nodeMap
     *         the node map
     * @param p_messageDirectory
     *         the message directory
     * @param p_requestMap
     *         the request map
     * @param p_lock
     *         the ReentrantLock
     * @param p_cond
     *         the Condition
     * @param p_messageCreator
     *         the incoming buffer storage and message creator
     * @param p_nioSelector
     *         the NIOSelector
     * @param p_osBufferSize
     *         the size of incoming and outgoing buffers
     * @param p_flowControlWindowSize
     *         the maximal number of ByteBuffer to schedule for sending/receiving
     * @throws IOException
     *         if the connection could not be created
     */
    NIOConnection(final short p_destination, final NodeMap p_nodeMap, final MessageDirectory p_messageDirectory, final RequestMap p_requestMap,
            final ReentrantLock p_lock, final Condition p_cond, final MessageCreator p_messageCreator, final NIOSelector p_nioSelector,
            final int p_osBufferSize, final int p_flowControlWindowSize) throws IOException {
        super(p_destination, p_nodeMap, p_messageDirectory, p_requestMap, p_flowControlWindowSize);

        m_osBufferSize = p_osBufferSize;

        m_outgoingChannel = SocketChannel.open();
        m_outgoingChannel.configureBlocking(false);
        m_outgoingChannel.socket().setSoTimeout(0);
        m_outgoingChannel.socket().setTcpNoDelay(true);
        m_outgoingChannel.socket().setReceiveBufferSize(32);
        m_outgoingChannel.socket().setSendBufferSize(m_osBufferSize);
        int sendBufferSize = m_outgoingChannel.socket().getSendBufferSize();
        if (sendBufferSize < m_osBufferSize) {
            // #if LOGGER >= WARN
            LOGGER.warn("Send buffer size could not be set properly. Check OS settings! Requested: %d, actual: %d", m_osBufferSize, sendBufferSize);
            // #endif /* LOGGER >= WARN */
        }

        m_outgoingChannel.connect(getNodeMap().getAddress(p_destination));

        m_messageCreator = p_messageCreator;
        m_nioSelector = p_nioSelector;

        m_outgoing = new OutgoingQueue(m_osBufferSize);

        m_connectionCondLock = p_lock;
        m_connectionCond = p_cond;

        m_sliceLock = new ReentrantLock(false);

        m_writeOperation = new ChangeOperationsRequest(this, NIOSelector.WRITE);
        m_flowControlOperation = new ChangeOperationsRequest(this, NIOSelector.FLOW_CONTROL);
    }

    /**
     * Creates an instance of NIOConnection (this node creates a new connection with another node)
     *
     * @param p_destination
     *         the destination
     * @param p_nodeMap
     *         the node map
     * @param p_messageDirectory
     *         the message directory
     * @param p_requestMap
     *         the request map
     * @param p_channel
     *         the socket channel
     * @param p_messageCreator
     *         the incoming buffer storage and message creator
     * @param p_nioSelector
     *         the NIOSelector
     * @param p_osBufferSize
     *         the size of outgoing buffer
     * @param p_flowControlWindowSize
     *         the maximal number of ByteBuffer to schedule for sending/receiving
     */
    NIOConnection(final short p_destination, final NodeMap p_nodeMap, final MessageDirectory p_messageDirectory, final RequestMap p_requestMap,
            final SocketChannel p_channel, final MessageCreator p_messageCreator, final NIOSelector p_nioSelector, final int p_osBufferSize,
            final int p_flowControlWindowSize) {
        super(p_destination, p_nodeMap, p_messageDirectory, p_requestMap, p_flowControlWindowSize);

        m_osBufferSize = p_osBufferSize;

        m_incomingChannel = p_channel;
        m_messageCreator = p_messageCreator;
        m_nioSelector = p_nioSelector;

        m_outgoing = new OutgoingQueue(m_osBufferSize);

        m_connectionCondLock = new ReentrantLock(false);
        m_connectionCond = m_connectionCondLock.newCondition();

        m_sliceLock = new ReentrantLock(false);

        m_writeOperation = new ChangeOperationsRequest(this, NIOSelector.WRITE);
        m_flowControlOperation = new ChangeOperationsRequest(this, NIOSelector.FLOW_CONTROL);
    }

    // Methods

    @Override
    public String toString() {
        String ret;

        ret = super.toString();
        if (m_outgoingChannel != null && m_outgoingChannel.isOpen()) {
            try {
                ret += ", address: " + m_outgoingChannel.getRemoteAddress() + "]\n";
            } catch (final IOException e) {
                e.printStackTrace();
            }
        } else if (m_incomingChannel != null && m_incomingChannel.isOpen()) {
            try {
                ret += ", address: " + m_incomingChannel.getRemoteAddress() + "]\n";
            } catch (final IOException e) {
                e.printStackTrace();
            }
        } else {
            ret += ", pending]\n";
        }

        return ret;
    }

    @Override
    protected boolean isOutgoingOpen() {
        return m_outgoingChannel != null && m_outgoingChannel.isOpen();
    }

    @Override
    protected boolean isIncomingOpen() {
        return m_incomingChannel != null && m_incomingChannel.isOpen();
    }

    @Override
    protected String getInputOutputQueueLength() {
        String ret = "";

        ret += "out: " + m_outgoing.size();
        ret += ", in: " + m_messageCreator.size();

        return ret;
    }

    /**
     * Register connect interest
     */
    protected void connect() {
        m_nioSelector.changeOperationInterestAsync(new ChangeOperationsRequest(this, NIOSelector.CONNECT));
    }

    /**
     * Returns whether the connection creation was aborted or not.
     *
     * @return true if connection creation was aborted, false otherwise
     */
    boolean isConnectionCreationAborted() {
        return m_aborted;
    }

    /**
     * Aborts the connection creation. Is called by selector thread.
     */
    void abortConnectionCreation() {
        m_aborted = true;
    }

    /**
     * Writes data to the connection
     *
     * @param p_message
     *         the AbstractMessage to send
     */
    @Override
    protected void doWrite(final AbstractMessage p_message) throws NetworkException {
        int messageSize = p_message.getTotalSize();

        if (messageSize > m_osBufferSize) {
            ByteBuffer data = p_message.getBuffer();
            m_sliceLock.lock();
            int size = data.limit();
            int currentSize = 0;
            while (currentSize < size) {
                currentSize = Math.min(currentSize + m_osBufferSize, size);
                data.limit(currentSize);
                writeToChannel(data.slice());
                data.position(currentSize);
            }
            m_sliceLock.unlock();
        } else {
            writeToChannel(p_message, messageSize);
        }
    }

    /**
     * Writes flow control data to the connection
     */
    @Override
    protected void doFlowControlWrite() throws NetworkException {
        m_nioSelector.changeOperationInterestAsync(m_flowControlOperation);
    }

    @Override
    protected boolean dataLeftToWrite() {
        return !m_outgoing.isEmpty();
    }

    @Override
    protected void wakeup() {
        m_nioSelector.getSelector().wakeup();
    }

    /**
     * Closes the connection immediately
     */
    @Override
    protected void doClose() {
        setClosingTimestamp();
        m_nioSelector.closeConnectionAsync(this);
    }

    /**
     * Closes the connection when there is no data left in transfer
     */
    @Override
    protected void doCloseGracefully() {
        if (!m_outgoing.isEmpty()) {
            // #if LOGGER >= DEBUG
            LOGGER.debug("Waiting for all scheduled messages to be sent over to be closed connection!");
            // #endif /* LOGGER >= DEBUG */
            long start = System.currentTimeMillis();
            while (!m_outgoing.isEmpty()) {
                Thread.yield();

                if (System.currentTimeMillis() - start > 10000) {
                    // #if LOGGER >= ERROR
                    LOGGER.debug("Waiting for all scheduled messages to be sent over aborted, timeout");
                    // #endif /* LOGGER >= ERROR */
                    break;
                }
            }
        }

        setClosingTimestamp();
        m_nioSelector.closeConnectionAsync(this);
    }

    void createOutgoingChannel(final short p_nodeID) throws IOException {
        m_outgoingChannel = SocketChannel.open();
        m_outgoingChannel.configureBlocking(false);
        m_outgoingChannel.socket().setSoTimeout(0);
        m_outgoingChannel.socket().setTcpNoDelay(true);
        m_outgoingChannel.socket().setReceiveBufferSize(32);
        m_outgoingChannel.socket().setSendBufferSize(m_osBufferSize);
        int sendBufferSize = m_outgoingChannel.socket().getSendBufferSize();
        if (sendBufferSize < m_osBufferSize) {
            // #if LOGGER >= WARN
            LOGGER.warn("Send buffer size could not be set properly. Check OS settings! Requested: %d, actual: %d", m_osBufferSize, sendBufferSize);
            // #endif /* LOGGER >= WARN */
        }

        m_outgoingChannel.connect(getNodeMap().getAddress(p_nodeID));
    }

    void bindIncomingChannel(final SocketChannel p_channel) throws IOException {
        m_incomingChannel = p_channel;
    }

    /**
     * Returns the pooled buffer from nio interface
     *
     * @param p_byteBuffer
     *         the pooled buffer
     */
    void returnReadBuffer(final ByteBuffer p_byteBuffer) {
        m_nioSelector.returnBuffer(p_byteBuffer);
    }

    /**
     * Returns the pooled buffer from outgoing queue
     *
     * @param p_byteBuffer
     *         the pooled buffer
     */
    void returnWriteBuffer(final ByteBuffer p_byteBuffer) {
        if (p_byteBuffer.capacity() == m_osBufferSize) {
            m_outgoing.returnBuffer(p_byteBuffer);
        }
    }

    /**
     * Returns the SocketChannel for outgoing messages
     *
     * @return the SocketChannel
     */
    SocketChannel getOutgoingChannel() {
        return m_outgoingChannel;
    }

    /**
     * Returns the SocketChannel for incoming messages
     *
     * @return the SocketChannel
     */
    SocketChannel getIncomingChannel() {
        return m_incomingChannel;
    }

    /**
     * Append an incoming ByteBuffer to the Queue
     *
     * @param p_buffer
     *         the ByteBuffer
     */
    void addIncoming(final ByteBuffer p_buffer) {
        // Avoid congestion by not allowing more than m_numberOfBuffers buffers to be cached for reading
        while (!m_messageCreator.pushJob(this, p_buffer)) {
            // #if LOGGER == TRACE
            LOGGER.trace("Network-Selector: Job queue is full!");
            // #endif /* LOGGER == TRACE */

            Thread.yield();
        }
    }

    /**
     * Get the next buffers to be sent
     *
     * @return Buffer array
     */
    ByteBuffer getOutgoingBytes() {
        return m_outgoing.popFront();
    }

    /**
     * Executes after the connection is established
     *
     * @param p_key
     *         the selection key
     * @throws IOException
     *         if the connection could not be accessed
     */

    void connected(final SelectionKey p_key) throws IOException {
        ByteBuffer temp;

        m_connectionCondLock.lock();
        temp = ByteBuffer.allocateDirect(2);
        temp.putShort(getNodeMap().getOwnNodeID());
        temp.flip();

        // Register first write access containing the NodeID
        m_outgoing.pushFront(temp);

        try {
            // Change operation (read <-> write) and/or connection
            p_key.interestOps(NIOSelector.WRITE);
        } catch (final CancelledKeyException ignore) {
            m_connectionCond.signalAll();
            m_connectionCondLock.unlock();
            return;
        }

        setConnected(true, true);

        m_connectionCond.signalAll();
        m_connectionCondLock.unlock();
    }

    /**
     * Prepend buffer to be written into the channel. Called if buffer could not be written completely.
     *
     * @param p_buffer
     *         Buffer
     */
    void addBuffer(final ByteBuffer p_buffer) {
        m_outgoing.pushFront(p_buffer);
    }

    /**
     * Enqueue buffer to be written into the channel
     *
     * @param p_buffer
     *         the buffer
     */
    private void writeToChannel(final ByteBuffer p_buffer) {
        m_outgoing.pushAndAggregateBuffers(p_buffer);
        m_nioSelector.changeOperationInterestAsync(m_writeOperation);
    }

    /**
     * Enqueue message to be written into the channel
     *
     * @param p_message
     *         the message
     */
    private void writeToChannel(final AbstractMessage p_message, final int p_messageSize) throws NetworkException {
        m_outgoing.pushAndAggregateBuffers(p_message, p_messageSize);
        m_nioSelector.changeOperationInterestAsync(m_writeOperation);
    }

}
