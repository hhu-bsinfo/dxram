
package de.hhu.bsinfo.menet;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Represents a network connections
 * @author Florian Klein 18.03.2012
 * @author Marc Ewert 14.10.2014
 */
public abstract class AbstractConnection {

	// Attributes
	private final DataHandler m_dataHandler;
	private final ByteStreamInterpreter m_streamInterpreter;

	private short m_destination;
	private NodeMap m_nodeMap;
	private final TaskExecutor m_taskExecutor;
	private MessageDirectory m_messageDirectory;

	private boolean m_connected;

	private DataReceiver m_listener;

	private long m_timestamp;

	private ReentrantLock m_lock;

	private int m_unconfirmedBytes;
	private int m_receivedBytes;

	private int m_flowControlWindowSize;

	private ReentrantLock m_flowControlCondLock;
	private Condition m_flowControlCond;

	// Constructors
	/**
	 * Creates an instance of AbstractConnection
	 * @param p_destination
	 *            the destination
	 * @param p_nodeMap
	 *            the node map
	 * @param p_taskExecutor
	 *            the task executer
	 * @param p_messageDirectory
	 *            the message directory
	 * @param p_flowControlWindowSize
	 *            the maximal number of ByteBuffer to schedule for sending/receiving
	 */
	AbstractConnection(final short p_destination, final NodeMap p_nodeMap, final TaskExecutor p_taskExecutor, final MessageDirectory p_messageDirectory,
			final int p_flowControlWindowSize) {
		this(p_destination, p_nodeMap, p_taskExecutor, p_messageDirectory, null, p_flowControlWindowSize);
	}

	/**
	 * Creates an instance of AbstractConnection
	 * @param p_destination
	 *            the destination
	 * @param p_listener
	 *            the ConnectionListener
	 * @param p_nodeMap
	 *            the node map
	 * @param p_taskExecutor
	 *            the task executer
	 * @param p_messageDirectory
	 *            the message directory
	 * @param p_flowControlWindowSize
	 *            the maximal number of ByteBuffer to schedule for sending/receiving
	 */
	AbstractConnection(final short p_destination, final NodeMap p_nodeMap, final TaskExecutor p_taskExecutor, final MessageDirectory p_messageDirectory,
			final DataReceiver p_listener, final int p_flowControlWindowSize) {
		assert p_destination != NodeID.INVALID_ID;

		m_dataHandler = new DataHandler();
		m_streamInterpreter = new ByteStreamInterpreter();

		m_destination = p_destination;
		m_nodeMap = p_nodeMap;
		m_taskExecutor = p_taskExecutor;
		m_messageDirectory = p_messageDirectory;

		m_connected = false;

		m_listener = p_listener;

		m_timestamp = 0;

		m_flowControlWindowSize = p_flowControlWindowSize;
		m_flowControlCondLock = new ReentrantLock(false);
		m_flowControlCond = m_flowControlCondLock.newCondition();

		m_lock = new ReentrantLock(false);
	}

	// Getters
	/**
	 * Checks if the connection is connected
	 * @return true if the connection is connected, false otherwise
	 */
	public final boolean isConnected() {
		boolean ret;

		m_lock.lock();
		ret = m_connected;
		m_lock.unlock();

		return ret;
	}

	/**
	 * Checks if the connection is connected
	 * @return true if the connection is connected, false otherwise
	 */
	public final boolean isCongested() {
		return m_unconfirmedBytes > m_flowControlWindowSize;
	}

	/**
	 * Get node map
	 * @return the NodeMap
	 */
	public final NodeMap getNodeMap() {
		return m_nodeMap;
	}

	/**
	 * Get the destination
	 * @return the destination
	 */
	public final short getDestination() {
		return m_destination;
	}

	/**
	 * Get the timestamp of the last access
	 * @return the timestamp of the last access
	 */
	public final long getTimestamp() {
		return m_timestamp;
	}

	// Setters
	/**
	 * Marks the connection as (not) connected
	 * @param p_connected
	 *            if true the connection is marked as connected, otherwise the connections marked as not connected
	 */
	protected final void setConnected(final boolean p_connected) {
		m_lock.lock();
		m_connected = p_connected;
		m_lock.unlock();
	}

	/**
	 * Set the ConnectionListener
	 * @param p_listener
	 *            the ConnectionListener
	 */
	public final void setListener(final DataReceiver p_listener) {
		m_listener = p_listener;
	}

	// Methods
	/**
	 * Reads messages from the connection
	 * @return a AbstractMessage which was received
	 * @throws IOException
	 *             if the message could not be read
	 */
	protected final ByteBuffer read() throws IOException {
		ByteBuffer ret;

		ret = doRead();

		m_receivedBytes += ret.remaining();
		m_flowControlCondLock.lock();
		if (m_receivedBytes > m_flowControlWindowSize * 0.8) {
			sendFlowControlMessage();
		}
		m_flowControlCondLock.unlock();

		m_timestamp = System.currentTimeMillis();

		return ret;
	}

	/**
	 * Reads messages from the connection
	 * @return a AbstractMessage which was received
	 * @throws IOException
	 *             if the message could not be read
	 */
	protected abstract ByteBuffer doRead() throws IOException;

	/**
	 * Writes data to the connection
	 * @param p_message
	 *            the AbstractMessage to send
	 * @throws IOException
	 *             if the data could not be written
	 */
	public final void write(final AbstractMessage p_message) throws IOException {
		m_flowControlCondLock.lock();
		while (m_unconfirmedBytes > m_flowControlWindowSize) {
			try {
				m_flowControlCond.await();
			} catch (final InterruptedException e) { /* ignore */}
		}
		try {
			m_unconfirmedBytes += p_message.getBuffer().remaining();
		} catch (final NetworkException e) {
			NetworkHandler.getLogger().error(getClass().getSimpleName(), "Could not send message: " + p_message, e);
			return;
		} finally {
			m_flowControlCondLock.unlock();
		}

		try {
			doWrite(p_message);
		} catch (final NetworkException e) {
			NetworkHandler.getLogger().error(getClass().getSimpleName(), "Could not send message: " + p_message, e);
			return;
		}

		m_timestamp = System.currentTimeMillis();
	}

	/**
	 * Writes data to the connection
	 * @param p_message
	 *            the AbstractMessage to send
	 * @throws NetworkException
	 *             if message buffer is too small
	 */
	protected abstract void doWrite(final AbstractMessage p_message) throws NetworkException;

	/**
	 * Closes the connection
	 */
	public final void close() {
		m_connected = false;

		doClose();
	}

	/**
	 * Closes the connection
	 */
	protected abstract void doClose();

	/**
	 * Called when the connection was closed.
	 */
	public final void cleanup() {
		if (!m_connected) {
			m_taskExecutor.purgeQueue(m_destination);
		}
	}

	/**
	 * Informs the ConnectionListener about a new message
	 * @param p_message
	 *            the new message
	 */
	protected void deliverMessage(final AbstractMessage p_message) {
		if (p_message instanceof FlowControlMessage) {
			handleFlowControlMessage((FlowControlMessage) p_message);
		} else {
			if (m_listener != null) {
				m_listener.newMessage(p_message);
			}
		}
	}

	/**
	 * Called when new data has received
	 */
	protected final void newData() {
		m_taskExecutor.execute(m_destination, m_dataHandler);
	}

	/**
	 * Confirm received bytes for the other node
	 */
	private void sendFlowControlMessage() {
		FlowControlMessage message;
		ByteBuffer messageBuffer;

		message = new FlowControlMessage(m_receivedBytes);
		try {
			messageBuffer = message.getBuffer();
		} catch (final NetworkException e) {
			NetworkHandler.getLogger().error(getClass().getSimpleName(), "Could not send flow control message", e);
			return;
		}

		// add sending bytes for consistency
		m_unconfirmedBytes += messageBuffer.remaining();

		try {
			doWrite(message);
		} catch (final NetworkException e) {
			NetworkHandler.getLogger().error(getClass().getSimpleName(), "Could not send flow control message", e);
			return;
		}

		// reset received bytes counter
		m_receivedBytes = 0;
	}

	/**
	 * Handles a received FlowControlMessage
	 * @param p_message
	 *            FlowControlMessage
	 */
	private void handleFlowControlMessage(final FlowControlMessage p_message) {
		m_flowControlCondLock.lock();
		m_unconfirmedBytes -= p_message.getConfirmedBytes();

		m_flowControlCond.signalAll();
		m_flowControlCondLock.unlock();
	}

	/**
	 * Get the String representation
	 * @return the String representation
	 */
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "[" + m_destination + ", " + m_connected + "]";
	}

	// Classes
	/**
	 * Manages for reacting to connections
	 * @author Marc Ewert 11.04.2014
	 */
	public interface DataReceiver {

		// Methods
		/**
		 * New messsage is available
		 * @param p_message
		 *            the message which has been received
		 */
		void newMessage(AbstractMessage p_message);
	}

	/**
	 * Reacts on incoming data
	 * @author Florian Klein 23.07.2013
	 * @author Marc Ewert 16.09.2014
	 */
	private class DataHandler implements Runnable {

		/**
		 * Default constructor
		 */
		DataHandler() {}

		// Methods
		@Override
		public void run() {
			ByteStreamInterpreter streamInterpreter;
			ByteBuffer buffer;
			ByteBuffer messageBuffer;
			AbstractMessage message;

			try {
				streamInterpreter = m_streamInterpreter;
				buffer = read();

				// could be null when an other thread has read the buffer before
				if (buffer != null) {
					while (buffer.hasRemaining()) {
						// Update the MessageCreator
						streamInterpreter.update(buffer);

						if (streamInterpreter.isMessageComplete()) {
							if (!streamInterpreter.isExceptionOccurred()) {
								messageBuffer = streamInterpreter.getMessageBuffer();

								message = createMessage(messageBuffer);

								if (message != null) {
									message.setDestination(m_nodeMap.getOwnNodeID());
									message.setSource(m_destination);
									deliverMessage(message);
								}
							}

							streamInterpreter.clear();
						}
					}
				}
			} catch (final IOException e) {
				NetworkHandler.getLogger().error(getClass().getSimpleName(), "ERROR::Could not access network connection", e);
			}
		}

		/**
		 * Create a message from a given buffer
		 * @param p_buffer
		 *            buffer containing a message
		 * @return message
		 */
		private AbstractMessage createMessage(final ByteBuffer p_buffer) {
			AbstractMessage message = null;

			p_buffer.flip();
			try {
				message = AbstractMessage.createMessageHeader(p_buffer, m_messageDirectory);
				// hack:
				// to avoid copying data multiple times, some responses use the same objects provided
				// with the request to directly write the data to them instead of creating a temporary
				// object in the response, de-serializing the data and then copying from the temporary object
				// to the object that should receive the data in the first place. (example DXRAM: get request/response)
				// This is only possible, if we have a reference to the original request within the response
				// while reading from the network byte buffer. But in this low level stage, we (usually) don't have
				// access to requests/responses. So we exploit the request map to get our corresponding request
				// before de-serializing the network buffer for every request.
				if (message instanceof AbstractResponse) {
					final AbstractResponse resp = (AbstractResponse) message;
					resp.setCorrespondingRequest(RequestMap.getRequest(resp));
				}

				try {
					message.readPayload(p_buffer);
				} catch (final BufferUnderflowException e) {
					throw new IOException("Read beyond message buffer", e);
				}

				if (p_buffer.position() < message.getPayloadLength() + AbstractMessage.HEADER_SIZE - AbstractMessage.PAYLOAD_SIZE_LENGTH) {
					throw new IOException("Message buffer is too large");
				}
			} catch (final Exception e) {
				if (message != null) {
					NetworkHandler.getLogger().error(getClass().getSimpleName(), "Unable to create message: " + message, e);
				} else {
					NetworkHandler.getLogger().error(getClass().getSimpleName(), "Unable to create message", e);
				}
			}

			return message;
		}
	}

	/**
	 * Creates ByteBuffers containing AbstractMessages from ByteBuffer-Chunks
	 * @author Florian Klein 09.03.2012
	 * @author Marc Ewert 28.10.2014
	 */
	private class ByteStreamInterpreter {

		// Attributes
		private ByteBuffer m_headerBytes;
		private ByteBuffer m_messageBytes;

		private Step m_step;

		private boolean m_exceptionOccurred;

		// Constructors
		/**
		 * Creates an instance of MessageCreator
		 */
		ByteStreamInterpreter() {
			m_headerBytes = ByteBuffer.allocate(AbstractMessage.HEADER_SIZE);
			clear();
		}

		// Getters
		/**
		 * Get the created Message
		 * @return the created Message
		 */
		public final ByteBuffer getMessageBuffer() {
			return m_messageBytes;
		}

		/**
		 * Checks if an Exception occurred
		 * @return true if an Exception occurred, false otherwise
		 */
		public final boolean isExceptionOccurred() {
			return m_exceptionOccurred;
		}

		// Methods
		/**
		 * Clear all data
		 */
		public void clear() {
			m_headerBytes.clear();
			m_step = Step.READ_HEADER;
			m_exceptionOccurred = false;
		}

		/**
		 * Updates the current data
		 * @param p_buffer
		 *            the ByteBuffer with new data
		 */
		public void update(final ByteBuffer p_buffer) {
			assert p_buffer != null;

			while (m_step != Step.DONE && p_buffer.hasRemaining()) {
				switch (m_step) {
				case READ_HEADER:
					readHeader(p_buffer);
					break;
				case READ_PAYLOAD:
					readPayload(p_buffer);
					break;
				default:
					break;
				}
			}
		}

		/**
		 * Reads the remaining message header
		 * @param p_buffer
		 *            the ByteBuffer with the data
		 */
		private void readHeader(final ByteBuffer p_buffer) {
			try {
				final int remaining = m_headerBytes.remaining();

				if (p_buffer.remaining() < remaining) {
					m_headerBytes.put(p_buffer);
					// Header partially filled
				} else {
					m_headerBytes.put(p_buffer.array(), p_buffer.position(), remaining);
					p_buffer.position(p_buffer.position() + remaining);

					// Header complete

					// Read payload size (copied at the end of m_headerBytes before)
					final int payloadSize = m_headerBytes.getInt(m_headerBytes.limit() - AbstractMessage.PAYLOAD_SIZE_LENGTH);

					// Create message buffer and copy header into (without payload size)
					m_messageBytes = ByteBuffer.allocate(AbstractMessage.HEADER_SIZE - AbstractMessage.PAYLOAD_SIZE_LENGTH + payloadSize);
					m_messageBytes.put(m_headerBytes.array(), 0, AbstractMessage.HEADER_SIZE - AbstractMessage.PAYLOAD_SIZE_LENGTH);

					if (payloadSize == 0) {
						// There is no payload -> message complete
						m_step = Step.DONE;
					} else {
						// Payload must be read next
						m_step = Step.READ_PAYLOAD;
					}
				}
			} catch (final Exception e) {
				NetworkHandler.getLogger().error(getClass().getSimpleName(), "Unable to read message header ", e);
				clear();
			}
		}

		/**
		 * Reads the message payload
		 * @param p_buffer
		 *            the ByteBuffer with the data
		 */
		private void readPayload(final ByteBuffer p_buffer) {
			try {
				final int remaining = m_messageBytes.remaining();

				if (p_buffer.remaining() < remaining) {
					m_messageBytes.put(p_buffer);
				} else {
					m_messageBytes.put(p_buffer.array(), p_buffer.position(), remaining);
					p_buffer.position(p_buffer.position() + remaining);
					m_step = Step.DONE;
				}
			} catch (final Exception e) {
				NetworkHandler.getLogger().error(getClass().getSimpleName(), "Unable to read message payload ", e);
				clear();
			}
		}

		/**
		 * Checks if Message is complete
		 * @return true if the Message is complete, false otherwise
		 */
		public boolean isMessageComplete() {
			return m_step == Step.DONE;
		}
	}

	/**
	 * Represents the steps in the creation process
	 * @author Florian Klein
	 *         09.03.2012
	 */
	private enum Step {

		// Constants
		READ_HEADER, READ_PAYLOAD, DONE

	}
}
