/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
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
    private static final int AGGREGATION_LIMIT = 4 * 1024;

    // Attributes
    private SocketChannel m_outgoingChannel;
    private SocketChannel m_incomingChannel;
    private NIOSelector m_nioSelector;
    private MessageCreator m_messageCreator;
    private ArrayDeque<ByteBuffer> m_outgoing;
    private int m_incomingBufferSize;
    private int m_outgoingBufferSize;
    private ReentrantLock m_connectionCondLock;
    private Condition m_connectionCond;

    private ReentrantLock m_outgoingLock;
    private Condition m_outgoingCond;
    private ChangeOperationsRequest m_writeOperation;
    private ChangeOperationsRequest m_flowControlOperation;

    private ReentrantLock m_sliceLock;
    private ReentrantLock m_writerLock;

    private int m_currentQueueSize;

    // Constructors

    /**
     * Creates an instance of NIOConnection (this node creates a new connection with another node)
     *
     * @param p_destination
     *     the destination
     * @param p_nodeMap
     *     the node map
     * @param p_messageDirectory
     *     the message directory
     * @param p_lock
     *     the ReentrantLock
     * @param p_cond
     *     the Condition
     * @param p_messageCreator
     *     the incoming buffer storage and message creator
     * @param p_nioSelector
     *     the NIOSelector
     * @param p_incomingBufferSize
     *     the size of incoming buffer
     * @param p_outgoingBufferSize
     *     the size of outgoing buffer
     * @param p_flowControlWindowSize
     *     the maximal number of ByteBuffer to schedule for sending/receiving
     * @throws IOException
     *     if the connection could not be created
     */
    NIOConnection(final short p_destination, final NodeMap p_nodeMap, final MessageDirectory p_messageDirectory, final ReentrantLock p_lock,
        final Condition p_cond, final MessageCreator p_messageCreator, final NIOSelector p_nioSelector, final int p_incomingBufferSize,
        final int p_outgoingBufferSize, final int p_flowControlWindowSize) throws IOException {
        super(p_destination, p_nodeMap, p_messageDirectory, p_flowControlWindowSize);

        m_incomingBufferSize = p_incomingBufferSize;
        m_outgoingBufferSize = p_outgoingBufferSize;

        m_outgoingChannel = SocketChannel.open();
        m_outgoingChannel.configureBlocking(false);
        m_outgoingChannel.socket().setSoTimeout(0);
        m_outgoingChannel.socket().setTcpNoDelay(true);
        m_outgoingChannel.socket().setReceiveBufferSize(32);
        m_outgoingChannel.socket().setSendBufferSize(m_outgoingBufferSize);

        m_outgoingChannel.connect(getNodeMap().getAddress(p_destination));

        m_messageCreator = p_messageCreator;
        m_nioSelector = p_nioSelector;

        m_outgoing = new ArrayDeque<>();
        m_currentQueueSize = 0;

        m_connectionCondLock = p_lock;
        m_connectionCond = p_cond;

        m_outgoingLock = new ReentrantLock(false);
        m_outgoingCond = m_outgoingLock.newCondition();

        m_sliceLock = new ReentrantLock(false);
        m_writerLock = new ReentrantLock(false);

        m_writeOperation = new ChangeOperationsRequest(this, NIOSelector.WRITE);
        m_flowControlOperation = new ChangeOperationsRequest(this, NIOSelector.FLOW_CONTROL);
    }

    /**
     * Creates an instance of NIOConnection (this node creates a new connection with another node)
     *
     * @param p_destination
     *     the destination
     * @param p_nodeMap
     *     the node map
     * @param p_messageDirectory
     *     the message directory
     * @param p_channel
     *     the socket channel
     * @param p_messageCreator
     *     the incoming buffer storage and message creator
     * @param p_nioSelector
     *     the NIOSelector
     * @param p_incomingBufferSize
     *     the size of incoming buffer
     * @param p_outgoingBufferSize
     *     the size of outgoing buffer
     * @param p_flowControlWindowSize
     *     the maximal number of ByteBuffer to schedule for sending/receiving
     */
    NIOConnection(final short p_destination, final NodeMap p_nodeMap, final MessageDirectory p_messageDirectory, final SocketChannel p_channel,
        final MessageCreator p_messageCreator, final NIOSelector p_nioSelector, final int p_incomingBufferSize, final int p_outgoingBufferSize,
        final int p_flowControlWindowSize) {
        super(p_destination, p_nodeMap, p_messageDirectory, p_flowControlWindowSize);

        m_incomingBufferSize = p_incomingBufferSize;
        m_outgoingBufferSize = p_outgoingBufferSize;

        m_incomingChannel = p_channel;
        m_messageCreator = p_messageCreator;
        m_nioSelector = p_nioSelector;

        m_outgoing = new ArrayDeque<>();
        m_currentQueueSize = 0;

        m_connectionCondLock = new ReentrantLock(false);
        m_connectionCond = m_connectionCondLock.newCondition();

        m_outgoingLock = new ReentrantLock(false);
        m_outgoingCond = m_outgoingLock.newCondition();

        m_sliceLock = new ReentrantLock(false);
        m_writerLock = new ReentrantLock(false);

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
    protected boolean isIncomingQueueFull() {
        return m_messageCreator.isFullLazy();
    }

    @Override
    protected String getInputOutputQueueLength() {
        String ret = "";

        m_outgoingLock.lock();
        ret += "out: " + m_outgoing.size();
        m_outgoingLock.unlock();

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
     * Writes data to the connection
     *
     * @param p_message
     *     the AbstractMessage to send
     */
    @Override
    protected void doWrite(final AbstractMessage p_message) throws NetworkException {
        ByteBuffer data = p_message.getBuffer();
        if (data.limit() > m_outgoingBufferSize) {
            m_sliceLock.lock();
            int size = data.limit();
            int currentSize = 0;
            while (currentSize < size) {
                currentSize = Math.min(currentSize + m_outgoingBufferSize, size);
                data.limit(currentSize);
                writeToChannel(data.slice());
                data.position(currentSize);
            }
            m_sliceLock.unlock();
        } else {
            writeToChannel(data);
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
        boolean ret;

        m_outgoingLock.lock();
        ret = !m_outgoing.isEmpty();
        m_outgoingLock.unlock();

        return ret;
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
        m_outgoingChannel.socket().setSendBufferSize(m_outgoingBufferSize);

        m_outgoingChannel.connect(getNodeMap().getAddress(p_nodeID));
    }

    void bindIncomingChannel(final SocketChannel p_channel) throws IOException {
        m_incomingChannel = p_channel;
    }

    /**
     * Returns the pooled buffer from nio interface
     *
     * @param p_byteBuffer
     *     the pooled buffer
     */
    void returnBuffer(final ByteBuffer p_byteBuffer) {
        m_nioSelector.returnBuffer(p_byteBuffer);
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
     *     the ByteBuffer
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
     * @param p_buffer
     *     buffer to gather in data
     * @return Buffer array
     */
    ByteBuffer getOutgoingBytes(final ByteBuffer p_buffer) {
        int length = 0;
        ByteBuffer buffer;
        ByteBuffer ret = null;

        m_outgoingLock.lock();
        buffer = m_outgoing.poll();
        if (buffer == null) {
            // The buffer for this write access was sent with previous call -> queue is empty -> do nothing
        } else if (buffer.position() != 0 || m_outgoing.isEmpty() || buffer.remaining() > AGGREGATION_LIMIT) {
            // This buffer was sent partially or this is the only buffer in queue or this buffer is reasonably large -> send this buffer alone
            ret = buffer;
            m_currentQueueSize -= buffer.remaining();
            m_outgoingCond.signalAll();
        } else {
            // This is a small buffer and there is more in queue -> aggregate buffers and send as one
            ret = p_buffer;
            ret.clear();

            length += buffer.remaining();
            ret.put(buffer);

            while (length < AGGREGATION_LIMIT && !m_outgoing.isEmpty()) {
                buffer = m_outgoing.poll();

                if (buffer.remaining() + length > m_outgoingBufferSize) {
                    // This buffer is too large -> write it next time
                    m_outgoing.addFirst(buffer);
                    break;
                }

                length += buffer.remaining();
                ret.put(buffer);
            }
            ret.flip();

            m_currentQueueSize -= length;
            m_outgoingCond.signalAll();
        }
        m_outgoingLock.unlock();

        return ret;
    }

    /**
     * Executes after the connection is established
     *
     * @throws IOException
     *     if the connection could not be accessed
     */

    void connected() throws IOException {
        ByteBuffer temp;

        m_connectionCondLock.lock();
        temp = ByteBuffer.allocate(2);
        temp.putShort(getNodeMap().getOwnNodeID());
        temp.flip();

        m_outgoingLock.lock();
        // Register first write access containing the NodeID
        m_outgoing.addFirst(temp);
        m_outgoingLock.unlock();

        // Change operation (read <-> write) and/or connection
        m_nioSelector.changeOperationInterestAsync(m_writeOperation);

        setConnected(true, true);

        m_outgoingLock.lock();
        m_outgoingCond.signalAll();
        m_outgoingLock.unlock();

        m_connectionCond.signalAll();
        m_connectionCondLock.unlock();
    }

    /**
     * Prepend buffer to be written into the channel. Called if buffer could not be written completely.
     *
     * @param p_buffer
     *     Buffer
     */
    void addBuffer(final ByteBuffer p_buffer) {
        m_outgoingLock.lock();
        m_outgoing.addFirst(p_buffer);
        m_currentQueueSize += p_buffer.remaining();
        m_outgoingLock.unlock();
    }

    /**
     * Enqueue buffer to be written into the channel
     *
     * @param p_buffer
     *     Buffer
     */
    private void writeToChannel(final ByteBuffer p_buffer) {
        m_writerLock.lock();
        m_outgoingLock.lock();
        while (m_currentQueueSize >= 2 * m_outgoingBufferSize || !isOutgoingConnected()) {
            try {
                m_outgoingCond.await();
            } catch (InterruptedException ignored) {
            }
        }

        m_outgoing.offer(p_buffer);
        m_currentQueueSize += p_buffer.limit();
        m_outgoingLock.unlock();

        // Change operation (read <-> write) and/or connection
        m_nioSelector.changeOperationInterestAsync(m_writeOperation);
        m_writerLock.unlock();
    }

}
