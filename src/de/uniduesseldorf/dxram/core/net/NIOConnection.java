
package de.uniduesseldorf.dxram.core.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHelper;
import de.uniduesseldorf.dxram.core.io.OutputHelper;
import de.uniduesseldorf.dxram.core.util.NodeID;

/**
 * Represents a network connection
 * @author Florian Klein 18.03.2012
 */
public class NIOConnection extends AbstractConnection {

	// Constants
	private static final int NUMBER_OF_BUFFERS = 250;

	// Attributes
	private SocketChannel m_channel;
	private NIOSelector m_nioSelector;

	private short m_nodeID;

	private final ArrayDeque<ByteBuffer> m_incoming;
	private final ArrayDeque<ByteBuffer> m_outgoing;
	private ReentrantLock m_incomingLock;
	private ReentrantLock m_outgoingAllLock;
	private ReentrantLock m_outgoingLock;

	private ReentrantLock m_connectionCondLock;
	private Condition m_connectionCond;

	// Constructors
	/**
	 * Creates an instance of NIOConnection (this node creates a new connection with another node)
	 * @param p_destination
	 *            the destination
	 * @param p_listener
	 *            the ConnectionListener
	 * @param p_lock
	 *            the ReentrantLock
	 * @param p_cond
	 *            the Condition
	 * @param p_nioSelector
	 *            the NIOSelector
	 * @throws IOException
	 *             if the connection could not be created
	 */
	protected NIOConnection(final short p_destination, final DataReceiver p_listener,
			final ReentrantLock p_lock, final Condition p_cond, final NIOSelector p_nioSelector) throws IOException {
		super(p_destination, p_listener);

		m_channel = SocketChannel.open();
		m_channel.configureBlocking(false);
		m_channel.socket().setSoTimeout(0);
		m_channel.socket().setTcpNoDelay(true);
		m_channel.socket().setSendBufferSize(NIOConnectionCreator.OUTGOING_BUFFER_SIZE);
		m_channel.socket().setReceiveBufferSize(NIOConnectionCreator.INCOMING_BUFFER_SIZE);

		m_nioSelector = p_nioSelector;
		m_nioSelector.changeOperationInterestAsync(this, SelectionKey.OP_CONNECT);

		final NodesConfigurationHelper helper = Core.getNodesConfiguration();
		m_nodeID = NodeID.getLocalNodeID();
		m_channel.connect(new InetSocketAddress(helper.getHost(p_destination), helper.getPort(p_destination)));

		m_incoming = new ArrayDeque<>();
		m_outgoing = new ArrayDeque<>();

		m_connectionCondLock = p_lock;
		m_connectionCond = p_cond;

		m_incomingLock = new ReentrantLock(false);
		m_outgoingAllLock = new ReentrantLock(false);
		m_outgoingLock = new ReentrantLock(false);
	}

	/**
	 * Creates an instance of NIOConnection (another node creates a new connection with this node)
	 * @param p_destination
	 *            the destination
	 * @param p_channel
	 *            the SocketChannel
	 * @param p_nioSelector
	 *            the NIOSelector
	 * @throws IOException
	 *             if the connection could not be created
	 */
	protected NIOConnection(final short p_destination, final SocketChannel p_channel, final NIOSelector p_nioSelector) throws IOException {
		super(p_destination);

		m_channel = p_channel;
		m_channel.configureBlocking(false);
		m_channel.socket().setSoTimeout(0);
		m_channel.socket().setTcpNoDelay(true);
		m_channel.socket().setSendBufferSize(NIOConnectionCreator.OUTGOING_BUFFER_SIZE);
		m_channel.socket().setReceiveBufferSize(NIOConnectionCreator.INCOMING_BUFFER_SIZE);

		m_nioSelector = p_nioSelector;

		m_incoming = new ArrayDeque<>();
		m_outgoing = new ArrayDeque<>();

		m_connectionCondLock = new ReentrantLock(false);
		m_connectionCond = m_connectionCondLock.newCondition();

		m_incomingLock = new ReentrantLock(false);
		m_outgoingAllLock = new ReentrantLock(false);
		m_outgoingLock = new ReentrantLock(false);
	}

	// Getter
	/**
	 * Returns the SocketChannel
	 * @return the SocketChannel
	 */
	public SocketChannel getChannel() {
		return m_channel;
	}

	// Methods
	/**
	 * Reads messages from the connection
	 * @return a AbstractMessage which was received
	 * @throws IOException
	 *             if the message could not be read
	 */
	@Override
	protected ByteBuffer doRead() {
		ByteBuffer ret;

		m_incomingLock.lock();
		ret = m_incoming.poll();
		m_incomingLock.unlock();

		return ret;
	}

	/**
	 * Append an incoming ByteBuffer to the Queue
	 * @param p_buffer
	 *            the ByteBuffer
	 */
	protected void addIncoming(final ByteBuffer p_buffer) {
		// Avoid congestion by not allowing more than NUMBER_OF_BUFFERS buffers to be cached for reading
		while (m_incoming.size() > NUMBER_OF_BUFFERS) {
			Thread.yield();
		}
		m_incomingLock.lock();
		m_incoming.offer(p_buffer);
		m_incomingLock.unlock();

		newData();
	}

	/**
	 * Writes data to the connection
	 * @param p_message
	 *            the AbstractMessage to send
	 * @throws IOException
	 *             if the data could not be written
	 */
	@Override
	protected void doWrite(final AbstractMessage p_message) {
		writeToChannel(p_message.getBuffer());
	}

	/**
	 * Enqueue buffer to be written into the channel
	 * @param p_buffer
	 *            Buffer
	 */
	private void writeToChannel(final ByteBuffer p_buffer) {
		// Avoid congestion by not allowing more than NUMBER_OF_BUFFERS messages to be buffered for writing
		while (m_outgoing.size() > NUMBER_OF_BUFFERS) {
			Thread.yield();
		}

		m_outgoingAllLock.lock();
		m_outgoingLock.lock();
		m_outgoing.offer(p_buffer);
		m_outgoingLock.unlock();

		// Change operation (read <-> write) and/or connection
		m_nioSelector.changeOperationInterestAsync(this, SelectionKey.OP_WRITE);
		m_outgoingAllLock.unlock();
	}

	/**
	 * Prepend buffer to be written into the channel. Called if buffer could not be written completely.
	 * @param p_buffer
	 *            Buffer
	 */
	void addBuffer(final ByteBuffer p_buffer) {
		System.out.println("got here");
		m_outgoingLock.lock();
		// Change operation request to OP_READ to read before trying to send the buffer again
		// m_nioSelector.changeOperationInterestAsync(this, SelectionKey.OP_WRITE);

		m_outgoing.addFirst(p_buffer);
		m_outgoingLock.unlock();
	}

	/**
	 * Get the next buffers to be sent
	 * @param p_buffer
	 *            buffer to gather in data
	 * @param p_bytes
	 *            number of bytes to be sent
	 * @return Buffer array
	 */
	protected ByteBuffer getOutgoingBytes(final ByteBuffer p_buffer, final int p_bytes) {
		int length = 0;
		int startPos = 0;
		boolean abort = false;
		ByteBuffer buffer;
		ByteBuffer ret = null;

		while (true) {
			m_outgoingLock.lock();
			if (m_outgoing.isEmpty()) {
				m_outgoingLock.unlock();
				break;
			}
			buffer = m_outgoing.poll();
			m_outgoingLock.unlock();

			// This is a left-over (see addBuffer())
			if (buffer.remaining() != 0 && buffer.position() != 0) {
				startPos = buffer.position();
				length += buffer.remaining();
				ret = buffer.duplicate();
				ret.limit(ret.capacity());
				abort = true;
			}

			// Skip when buffer is completed
			if (buffer == null || !buffer.hasRemaining()) {
				continue;
			}

			// Append when limit will not be reached
			if (length + buffer.remaining() + startPos <= p_bytes) {
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

		if (ret != null && !abort) {
			ret.position(0);
			if (length < ret.capacity()) {
				ret.limit(length);
			}
		}

		return ret;
	}

	/**
	 * Closes the connection
	 */
	@Override
	protected void doClose() {
		m_nioSelector.closeConnectionAsync(this);
	}

	/**
	 * Executes after the connection is established
	 * @throws IOException
	 *             if the connection could not be accessed
	 */
	protected void connected() throws IOException {
		ByteBuffer temp;

		m_connectionCondLock.lock();
		try {
			m_channel.socket().setSendBufferSize(NIOConnectionCreator.OUTGOING_BUFFER_SIZE);
			m_channel.socket().setReceiveBufferSize(NIOConnectionCreator.INCOMING_BUFFER_SIZE);
		} catch (final IOException e) {
			m_connectionCondLock.unlock();
			throw e;
		}

		temp = ByteBuffer.allocate(2);
		OutputHelper.writeNodeID(temp, m_nodeID);
		temp.flip();

		writeToChannel(temp);

		setConnected(true);

		m_connectionCond.signalAll();
		m_connectionCondLock.unlock();
	}

}
