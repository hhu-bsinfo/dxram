
package de.uniduesseldorf.menet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import de.uniduesseldorf.utils.Contract;

/**
 * Represents a network connections
 * @author Florian Klein 18.03.2012
 * @author Marc Ewert 14.10.2014
 */
public abstract class AbstractConnection {

	// Constants
	private static final Logger LOGGER = Logger.getLogger(AbstractConnection.class);

	// Attributes
	private final DataHandler m_dataHandler;
	private final MessageCreator m_messageCreator;
	private final ByteStreamInterpreter m_streamInterpreter;

	private short m_destination;
	private NodeMap m_nodeMap;
	private final TaskExecutor m_taskExecutor;

	private boolean m_connected;

	private DataReceiver m_listener;

	private int m_rating;
	private long m_timestamp;

	private ReentrantLock m_lock;

	// Constructors
	/**
	 * Creates an instance of AbstractConnection
	 * @param p_destination
	 *            the destination
	 */
	AbstractConnection(final short p_destination, final NodeMap p_nodeMap, final TaskExecutor p_taskExecutor, final MessageDirectory p_messageDirectory) {
		this(p_destination, p_nodeMap, p_taskExecutor, p_messageDirectory, null);
	}

	/**
	 * Creates an instance of AbstractConnection
	 * @param p_destination
	 *            the destination
	 * @param p_listener
	 *            the ConnectionListener
	 */
	AbstractConnection(final short p_destination, final NodeMap p_nodeMap, final TaskExecutor p_taskExecutor, final MessageDirectory p_messageDirectory, final DataReceiver p_listener) {
		NodeID.check(p_destination);

		m_dataHandler = new DataHandler();
		m_messageCreator = new MessageCreator(p_messageDirectory);
		m_streamInterpreter = new ByteStreamInterpreter();

		m_destination = p_destination;
		m_nodeMap = p_nodeMap;
		m_taskExecutor = p_taskExecutor;
		
		m_connected = false;

		m_listener = p_listener;

		m_rating = 0;
		m_timestamp = 0;

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
	 * Get the destination
	 * @return the destination
	 */
	public final short getDestination() {
		return m_destination;
	}

	/**
	 * Get the rating
	 * @return the rating
	 */
	public final int getRating() {
		return m_rating;
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
		doWrite(p_message);

		m_timestamp = System.currentTimeMillis();

		incRating(p_message.getRatingValue());
	}

	/**
	 * Writes data to the connection
	 * @param p_message
	 *            the AbstractMessage to send
	 * @throws IOException
	 *             if the data could not be written
	 */
	protected abstract void doWrite(final AbstractMessage p_message) throws IOException;

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
			m_taskExecutor.purgeQueue(m_destination);
		}
	}

	/**
	 * Increases the rating
	 * @param p_value
	 *            the value to add to the rating
	 */
	final void incRating(final byte p_value) {
		m_lock.lock();
		m_rating += p_value;
		m_lock.unlock();
	}

	/**
	 * Informs the ConnectionListener about a new message
	 * @param p_message
	 *            the new message
	 */
	protected void deliverMessage(final AbstractMessage p_message) {
		if (m_listener != null) {
			m_listener.newMessage(p_message);
		}
	}

	/**
	 * Called when new data has received
	 */
	protected final void newData() {
		m_taskExecutor.execute(m_destination, m_dataHandler);
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
								m_messageCreator.newData(messageBuffer);
							}

							streamInterpreter.clear();
						}
					}
				}
			} catch (final IOException e) {
				LOGGER.error("ERROR::Could not access network connection", e);
			}
		}
	}

	/**
	 * Creates messages from ByteBuffers
	 * @author Marc Ewert 28.10.2014
	 */
	private class MessageCreator implements Runnable {

		// Attributes
		private MessageDirectory m_messageDirectory;
		private final ArrayDeque<ByteBuffer> m_buffers;
		private ReentrantLock m_buffersLock;

		/**
		 * Default constructor
		 */
		MessageCreator(final MessageDirectory p_messageDirectory) {
			m_messageDirectory = p_messageDirectory;
			m_buffers = new ArrayDeque<>();
			m_buffersLock = new ReentrantLock(false);
		}

		/**
		 * Append a new buffer to create a message
		 * @param p_buffer
		 *            new buffer
		 */
		public void newData(final ByteBuffer p_buffer) {
			m_buffersLock.lock();
			m_buffers.offer(p_buffer);
			m_buffersLock.unlock();

			m_taskExecutor.execute(this);
		}

		@Override
		public void run() {
			AbstractMessage message;
			ByteBuffer buffer;

			m_buffersLock.lock();
			buffer = m_buffers.poll();
			m_buffersLock.unlock();

			message = createMessage(buffer);

			if (message != null) {
				message.setDestination(m_nodeMap.getOwnNodeID());
				message.setSource(m_destination);

				incRating(message.getRatingValue());
				deliverMessage(message);
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
				message = AbstractMessage.createMessageHeader(buffer, m_messageDirectory);
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
			while (m_headerBytes.hasRemaining() && p_buffer.hasRemaining()) {
				m_headerBytes.put(p_buffer.get());
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
			while (m_messageBytes.hasRemaining() && p_buffer.hasRemaining()) {
				m_messageBytes.put(p_buffer.get());
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
}
