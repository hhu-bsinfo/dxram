package de.hhu.bsinfo.ethnet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
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

    // Attributes
    private SocketChannel m_channel;
    private NIOSelector m_nioSelector;

    private MessageCreator m_messageCreator;
    private ArrayDeque<ByteBuffer> m_outgoing;

    private int m_incomingBufferSize;
    private int m_outgoingBufferSize;

    private int m_numberOfBuffersPerConnection;

    private ReentrantLock m_connectionCondLock;
    private Condition m_connectionCond;

    private ReentrantLock m_outgoingLock;

    private ChangeOperationsRequest m_writeOperation;

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
     * @param p_numberOfBuffersPerConnection
     *     the number of buffers to schedule
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
        final Condition p_cond, final MessageCreator p_messageCreator, final NIOSelector p_nioSelector, final int p_numberOfBuffersPerConnection,
        final int p_incomingBufferSize, final int p_outgoingBufferSize, final int p_flowControlWindowSize) throws IOException {
        super(p_destination, p_nodeMap, p_messageDirectory, p_flowControlWindowSize);

        m_incomingBufferSize = p_incomingBufferSize;
        m_outgoingBufferSize = p_outgoingBufferSize;

        m_numberOfBuffersPerConnection = p_numberOfBuffersPerConnection;

        m_channel = SocketChannel.open();
        m_channel.configureBlocking(false);
        m_channel.socket().setSoTimeout(0);
        m_channel.socket().setTcpNoDelay(true);
        m_channel.socket().setReceiveBufferSize(m_incomingBufferSize);
        m_channel.socket().setSendBufferSize(m_outgoingBufferSize);

        m_channel.connect(getNodeMap().getAddress(p_destination));

        m_messageCreator = p_messageCreator;
        m_nioSelector = p_nioSelector;

        m_outgoing = new ArrayDeque<>(m_numberOfBuffersPerConnection / 8);

        m_connectionCondLock = p_lock;
        m_connectionCond = p_cond;

        m_outgoingLock = new ReentrantLock(false);

        m_writeOperation = new ChangeOperationsRequest(this, SelectionKey.OP_WRITE);
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
     * @param p_numberOfBuffersPerConnection
     *     the number of buffers to schedule
     * @param p_incomingBufferSize
     *     the size of incoming buffer
     * @param p_outgoingBufferSize
     *     the size of outgoing buffer
     * @param p_flowControlWindowSize
     *     the maximal number of ByteBuffer to schedule for sending/receiving
     * @throws IOException
     *     if the connection could not be created
     */
    NIOConnection(final short p_destination, final NodeMap p_nodeMap, final MessageDirectory p_messageDirectory, final SocketChannel p_channel,
        final MessageCreator p_messageCreator, final NIOSelector p_nioSelector, final int p_numberOfBuffersPerConnection, final int p_incomingBufferSize,
        final int p_outgoingBufferSize, final int p_flowControlWindowSize) throws IOException {
        super(p_destination, p_nodeMap, p_messageDirectory, p_flowControlWindowSize);

        m_incomingBufferSize = p_incomingBufferSize;
        m_outgoingBufferSize = p_outgoingBufferSize;

        m_numberOfBuffersPerConnection = p_numberOfBuffersPerConnection;

        m_channel = p_channel;
        m_channel.configureBlocking(false);
        m_channel.socket().setSoTimeout(0);
        m_channel.socket().setTcpNoDelay(true);
        m_channel.socket().setReceiveBufferSize(m_incomingBufferSize);
        m_channel.socket().setSendBufferSize(m_outgoingBufferSize);

        m_messageCreator = p_messageCreator;
        m_nioSelector = p_nioSelector;

        m_outgoing = new ArrayDeque<>();

        m_connectionCondLock = new ReentrantLock(false);
        m_connectionCond = m_connectionCondLock.newCondition();

        m_outgoingLock = new ReentrantLock(false);

        m_writeOperation = new ChangeOperationsRequest(this, SelectionKey.OP_WRITE);
    }

    // Getter

    /**
     * Returns the SocketChannel
     *
     * @return the SocketChannel
     */
    SocketChannel getChannel() {
        return m_channel;
    }

    // Methods

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

    @Override
    public String toString() {
        String ret;

        ret = super.toString();
        if (m_channel.isOpen()) {
            try {
                ret += ", address: " + m_channel.getRemoteAddress() + "]\n";
            } catch (final IOException e) {
                e.printStackTrace();
            }
        }

        return ret;
    }

    /**
     * Register connect interest
     */
    protected void connect() {
        m_nioSelector.changeOperationInterestAsync(new ChangeOperationsRequest(this, SelectionKey.OP_CONNECT));
    }

    /**
     * Writes data to the connection
     *
     * @param p_message
     *     the AbstractMessage to send
     */
    @Override
    protected void doWrite(final AbstractMessage p_message) throws NetworkException {
        writeToChannel(p_message.getBuffer());
    }

    /**
     * Writes data to the connection
     *
     * @param p_message
     *     the AbstractMessage to send
     */
    @Override
    protected void doForceWrite(final AbstractMessage p_message) throws NetworkException {
        writeToChannelForced(p_message.getBuffer());
    }

    @Override
    protected boolean dataLeftToWrite() {
        boolean ret;

        m_outgoingLock.lock();
        ret = !m_outgoing.isEmpty();
        m_outgoingLock.unlock();

        return ret;
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

    /**
     * Append an incoming ByteBuffer to the Queue
     *
     * @param p_buffer
     *     the ByteBuffer
     */
    void addIncoming(final ByteBuffer p_buffer) {
        // Avoid congestion by not allowing more than m_numberOfBuffers buffers to be cached for reading
        while (!m_messageCreator.pushJob(this, p_buffer)) {
            Thread.yield();
        }
    }

    /**
     * Get the next buffers to be sent
     *
     * @param p_buffer
     *     buffer to gather in data
     * @param p_bytes
     *     number of bytes to be sent
     * @return Buffer array
     */
    ByteBuffer getOutgoingBytes(final ByteBuffer p_buffer, final int p_bytes) {
        int length = 0;
        ByteBuffer buffer;
        ByteBuffer ret = null;

        while (true) {
            m_outgoingLock.lock();
            buffer = m_outgoing.poll();
            m_outgoingLock.unlock();
            if (buffer == null) {
                break;
            }

            // Append when limit will not be reached
            if (length + buffer.remaining() <= p_bytes) {
                length += buffer.remaining();

                if (ret == null) {
                    p_buffer.clear();
                    ret = p_buffer;
                }
                ret.put(buffer);
            } else {
                // Append when limit has exceeded but buffer is still empty
                if (ret == null) {
                    length += buffer.remaining();
                    ret = buffer;
                } else {
                    // Write back buffer for next sending call
                    m_outgoingLock.lock();
                    m_outgoing.addFirst(buffer);
                    m_outgoingLock.unlock();
                }
                break;
            }
        }

        if (ret != null) {
            ret.position(0);
            if (length < ret.capacity()) {
                ret.limit(length);
            }
        }

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
        try {
            m_channel.socket().setReceiveBufferSize(m_incomingBufferSize);
            m_channel.socket().setSendBufferSize(m_outgoingBufferSize);
        } catch (final IOException e) {
            m_connectionCondLock.unlock();
            throw e;
        }

        temp = ByteBuffer.allocate(2);
        temp.putShort(getNodeMap().getOwnNodeID());
        temp.flip();

        writeToChannel(temp);

        setConnected(true);

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
        m_outgoingLock.unlock();
    }

    /**
     * Enqueue buffer to be written into the channel
     *
     * @param p_buffer
     *     Buffer
     */
    private void writeToChannel(final ByteBuffer p_buffer) {
        m_outgoingLock.lock();
        while (m_outgoing.size() == m_numberOfBuffersPerConnection) {
            m_outgoingLock.unlock();
            Thread.yield();
            m_outgoingLock.lock();
        }

        m_outgoing.offer(p_buffer);
        m_outgoingLock.unlock();

        // Change operation (read <-> write) and/or connection
        m_nioSelector.changeOperationInterestAsync(m_writeOperation);
    }

    /**
     * Enqueue buffer to be written into the channel (never delays)
     *
     * @param p_buffer
     *     Buffer
     */
    private void writeToChannelForced(final ByteBuffer p_buffer) {
        m_outgoingLock.lock();
        m_outgoing.offer(p_buffer);
        m_outgoingLock.unlock();

        // Change operation (read <-> write) and/or connection
        m_nioSelector.changeOperationInterestAsync(m_writeOperation);
    }

}
