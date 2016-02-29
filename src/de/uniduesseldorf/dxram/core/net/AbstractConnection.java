
package de.uniduesseldorf.dxram.core.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.util.NodeID;
import de.uniduesseldorf.dxram.utils.Contract;

/**
 * Represents a network connections
 * @author Florian Klein 18.03.2012
 * @author Marc Ewert 14.10.2014
 */
abstract class AbstractConnection {

	// Constants
	private static final Logger LOGGER = Logger.getLogger(AbstractConnection.class);
	private static final TaskExecutor EXECUTOR = TaskExecutor.getDefaultExecutor();
	protected static final int FLOW_CONTROL_LIMIT = 1 * 1024 * 1024;

	// Attributes
	private final DataHandler m_dataHandler;
	private final ByteStreamInterpreter m_streamInterpreter;

	private short m_destination;
	private boolean m_connected;
	private DataReceiver m_listener;
	private long m_timestamp;
	private ReentrantLock m_lock;

	private int m_unconfirmedBytes;
	private int m_receivedBytes;

	private ReentrantLock m_flowControlCondLock;
	private Condition m_flowControlCond;

	// Constructors
	/**
	 * Creates an instance of AbstractConnection
	 * @param p_destination
	 *            the destination
	 */
	AbstractConnection(final short p_destination) {
		this(p_destination, null);
	}

	/**
	 * Creates an instance of AbstractConnection
	 * @param p_destination
	 *            the destination
	 * @param p_listener
	 *            the ConnectionListener
	 */
	AbstractConnection(final short p_destination, final DataReceiver p_listener) {
		NodeID.check(p_destination);

		m_dataHandler = new DataHandler();
		m_streamInterpreter = new ByteStreamInterpreter();

		m_destination = p_destination;
		m_connected = false;
		m_listener = p_listener;
		m_timestamp = 0;

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
		return m_unconfirmedBytes > FLOW_CONTROL_LIMIT * 0.9; // Too conservative?
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
		if (m_receivedBytes > FLOW_CONTROL_LIMIT * 0.8) {
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
		while (m_unconfirmedBytes > FLOW_CONTROL_LIMIT) {
			try {
				m_flowControlCond.await();
			} catch (final InterruptedException e) { /* ignore */}
		}

		m_unconfirmedBytes += p_message.getBuffer().remaining();
		m_flowControlCondLock.unlock();

		doWrite(p_message);

		m_timestamp = System.currentTimeMillis();
	}

	/**
	 * Writes data to the connection
	 * @param p_message
	 *            the AbstractMessage to send
	 * @throws IOException
	 *             if the data could not be written
	 */
	protected abstract void doWrite(final AbstractMessage p_message);

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
		if (m_connected) {
			System.out.println("Connection to clean up is still connected");
		} else {
			EXECUTOR.purgeQueue(m_destination);
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
		EXECUTOR.execute(m_destination, m_dataHandler);
	}

	/**
	 * Confirm received bytes for the other node
	 */
	private void sendFlowControlMessage() {
		FlowControlMessage message;
		ByteBuffer messageBuffer;

		message = new FlowControlMessage(m_receivedBytes);
		messageBuffer = message.getBuffer();

		// add sending bytes for consistency
		m_unconfirmedBytes += messageBuffer.remaining();

		doWrite(message);

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
									message.setDestination(NodeID.getLocalNodeID());
									message.setSource(m_destination);
									deliverMessage(message);
								}
							}

							streamInterpreter.clear();
						}
					}
				}
			} catch (final IOException e) {
				LOGGER.error("ERROR::Could not access network connection", e);
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
			ByteBuffer buffer;

			p_buffer.flip();
			buffer = p_buffer.asReadOnlyBuffer();

			try {
				message = AbstractMessage.createMessageHeader(buffer);
				message.readPayload(buffer);
			} catch (final Exception e) {
				LOGGER.error("ERROR::Unable to create message", e);
			}

			return message;
		}
	}

	/**
	 * Creates ByteBuffers containing AbstractMessages from ByteBuffer-Chunks
	 * @author Florian Klein 09.03.2012
	 * @author Marc Ewert 28.10.2014
	 */
	private static class ByteStreamInterpreter {

		// Constants
		private static final Logger LOGGER = Logger.getLogger(ByteStreamInterpreter.class);
		private static final int HEADER_OFFSET = AbstractMessage.BYTES_PAYLOAD_SIZE;

		// Attributes
		private int m_bytesRead;
		private int m_payloadSize;
		private ByteBuffer m_headerBytes;
		private ByteBuffer m_messageBytes;

		private Step m_step;

		private boolean m_exceptionOccurred;

		// Constructors
		/**
		 * Creates an instance of MessageCreator
		 */
		ByteStreamInterpreter() {
			m_headerBytes = ByteBuffer.allocateDirect(AbstractMessage.HEADER_SIZE - HEADER_OFFSET);
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
			m_bytesRead = 0;
			m_payloadSize = 0;
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
			Contract.checkNotNull(p_buffer, "no buffer given");

			try {
				while (m_step != Step.DONE && p_buffer.hasRemaining()) {
					switch (m_step) {
					case READ_HEADER:
						readHeader(p_buffer);
						break;
					case READ_PAYLOAD_SIZE:
						readPayloadSize(p_buffer);
						break;
					case READ_PAYLOAD:
						readPayload(p_buffer);
						break;
					default:
						break;
					}
				}
			} catch (final Exception e) {
				LOGGER.error("ERROR::Unable to create Message ", e);
				clear();
			}
		}

		/**
		 * Reads the remaining message header
		 * without the 4 bytes for payload length
		 * @param p_buffer
		 *            the ByteBuffer with the data
		 */
		private void readHeader(final ByteBuffer p_buffer) {
			final int remaining = m_headerBytes.remaining();

			if (p_buffer.remaining() <= remaining) {
				m_headerBytes.put(p_buffer);
			} else {
				m_headerBytes.put(p_buffer.array(), p_buffer.position(), remaining);
				p_buffer.position(p_buffer.position() + remaining);
			}

			if (!m_headerBytes.hasRemaining()) {
				m_step = Step.READ_PAYLOAD_SIZE;
			}

			m_bytesRead = m_headerBytes.limit() - m_headerBytes.remaining();
		}

		/**
		 * Reads the size of the message payload
		 * @param p_buffer
		 *            the ByteBuffer with the data
		 */
		private void readPayloadSize(final ByteBuffer p_buffer) {
			while (m_bytesRead < AbstractMessage.HEADER_SIZE && p_buffer.hasRemaining()) {
				m_payloadSize = m_payloadSize << 8 | p_buffer.get() & 0xFF;

				m_bytesRead++;
			}

			if (m_bytesRead == AbstractMessage.HEADER_SIZE) {
				m_messageBytes = ByteBuffer.allocate(m_headerBytes.limit() + m_payloadSize);

				m_headerBytes.flip();
				m_messageBytes.put(m_headerBytes);

				if (m_payloadSize == 0) {
					// Payload data is complete
					m_step = Step.DONE;
				} else {
					m_step = Step.READ_PAYLOAD;
				}
			}
		}

		/**
		 * Reads the message payload
		 * @param p_buffer
		 *            the ByteBuffer with the data
		 */
		private void readPayload(final ByteBuffer p_buffer) {
			final int remaining = m_messageBytes.remaining();

			if (p_buffer.remaining() <= remaining) {
				m_messageBytes.put(p_buffer);
			} else {
				m_messageBytes.put(p_buffer.array(), p_buffer.position(), remaining);
				p_buffer.position(p_buffer.position() + remaining);
			}

			if (!m_messageBytes.hasRemaining()) {
				m_step = Step.DONE;
			}
		}

		/**
		 * Checks if Message is complete
		 * @return true if the Message is complete, false otherwise
		 */
		public boolean isMessageComplete() {
			return m_step == Step.DONE;
		}

		// Classes
		/**
		 * Represents the steps in the creation process
		 * @author Florian Klein
		 *         09.03.2012
		 */
		private enum Step {

			// Constants
			READ_HEADER, READ_PAYLOAD_SIZE, READ_PAYLOAD, DONE

		}
	}

	/**
	 * Used to confirm received bytes
	 * @author Marc Ewert 14.10.2014
	 */
	public static final class FlowControlMessage extends AbstractMessage {

		public static final byte TYPE = 0;
		public static final byte SUBTYPE = 1;

		private int m_confirmedBytes;

		/**
		 * Default constructor for serialization
		 */
		FlowControlMessage() {}

		/**
		 * Create a new Message for confirming received bytes.
		 * @param p_confirmedBytes
		 *            number of received bytes
		 */
		FlowControlMessage(final int p_confirmedBytes) {
			super((short) 0, TYPE, SUBTYPE, true);
			m_confirmedBytes = p_confirmedBytes;
		}

		/**
		 * Get number of confirmed bytes
		 * @return
		 *         the number of confirmed bytes
		 */
		public int getConfirmedBytes() {
			return m_confirmedBytes;
		}

		@Override
		protected void readPayload(final ByteBuffer p_buffer) {
			m_confirmedBytes = p_buffer.getInt();
		}

		@Override
		protected void writePayload(final ByteBuffer p_buffer) {
			p_buffer.putInt(m_confirmedBytes);
		}

		@Override
		protected int getPayloadLength() {
			return 4;
		}
	}
}
