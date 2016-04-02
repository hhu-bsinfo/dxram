
package de.hhu.bsinfo.menet;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Represents a network message
 * @author Florian Klein 09.03.2012
 * @author Marc Ewert 18.09.2014
 */
public abstract class AbstractMessage {

	// Constants
	static final byte BYTES_PAYLOAD_SIZE = 4;

	public static final int INVALID_MESSAGE_ID = -1;
	public static final byte DEFAULT_TYPE = 0;
	public static final byte DEFAULT_SUBTYPE = 0;
	public static final byte DEFAULT_STATUS_CODE = 0;
	public static final boolean DEFAULT_EXCLUSIVITY_VALUE = false;
	public static final byte DEFAULT_RATING_VALUE = 1;

	public static final byte HEADER_SIZE = 11;

	// Attributes
	private int m_messageID;
	private short m_source;
	private short m_destination;
	private byte m_type;
	private byte m_subtype;
	private boolean m_exclusivity;
	// status code for all messages to indicate success, errors etc.
	private byte m_statusCode;

	private static int m_nextMessageID = 1;
	private static ReentrantLock m_lock = new ReentrantLock(false);

	// Constructors
	/**
	 * Creates an instance of Message
	 */
	protected AbstractMessage() {
		m_messageID = INVALID_MESSAGE_ID;
		m_source = NodeID.INVALID_ID;
		m_destination = NodeID.INVALID_ID;
		m_type = DEFAULT_TYPE;
		m_subtype = DEFAULT_SUBTYPE;

		m_exclusivity = DEFAULT_EXCLUSIVITY_VALUE;

		m_statusCode = 0;
	}

	/**
	 * Creates an instance of Message
	 * @param p_destination
	 *            the destination
	 * @param p_type
	 *            the message type
	 * @param p_subtype
	 *            the message subtype
	 */
	public AbstractMessage(final short p_destination, final byte p_type, final byte p_subtype) {
		this(getNextMessageID(), p_destination, p_type, p_subtype, DEFAULT_EXCLUSIVITY_VALUE, DEFAULT_STATUS_CODE);
	}

	/**
	 * Creates an instance of Message
	 * @param p_destination
	 *            the destination
	 * @param p_type
	 *            the message type
	 * @param p_subtype
	 *            the message subtype
	 * @param p_exclusivity
	 *            whether this message type allows parallel execution
	 */
	public AbstractMessage(final short p_destination, final byte p_type, final byte p_subtype, final boolean p_exclusivity) {
		this(getNextMessageID(), p_destination, p_type, p_subtype, p_exclusivity, DEFAULT_STATUS_CODE);
	}

	/**
	 * Creates an instance of Message
	 * @param p_messageID
	 *            the messageID
	 * @param p_destination
	 *            the destination
	 * @param p_type
	 *            the message type
	 * @param p_subtype
	 *            the message subtype
	 */
	protected AbstractMessage(final int p_messageID, final short p_destination, final byte p_type, final byte p_subtype) {
		this(p_messageID, p_destination, p_type, p_subtype, DEFAULT_EXCLUSIVITY_VALUE, DEFAULT_STATUS_CODE);
	}

	/**
	 * Creates an instance of Message
	 * @param p_messageID
	 *            the messageID
	 * @param p_destination
	 *            the destination
	 * @param p_type
	 *            the message type
	 * @param p_subtype
	 *            the message subtype
	 * @param p_exclusivity
	 *            whether this is an exclusive message or not
	 * @param p_statusCode
	 *            the status code
	 */
	protected AbstractMessage(final int p_messageID, final short p_destination, final byte p_type, final byte p_subtype, final boolean p_exclusivity,
			final byte p_statusCode) {
		assert p_destination != NodeID.INVALID_ID;

		m_messageID = p_messageID;
		m_source = -1;
		m_destination = p_destination;
		m_type = p_type;
		m_subtype = p_subtype;

		m_exclusivity = p_exclusivity;
		m_statusCode = p_statusCode;
	}

	// Getters
	/**
	 * Get the messageID
	 * @return the messageID
	 */
	public final int getMessageID() {
		return m_messageID;
	}

	/**
	 * Get the source
	 * @return the source
	 */
	public final short getSource() {
		return m_source;
	}

	/**
	 * Get the destination
	 * @return the destination
	 */
	public final short getDestination() {
		return m_destination;
	}

	/**
	 * Get the message type
	 * @return the message type
	 */
	public final byte getType() {
		return m_type;
	}

	/**
	 * Get the message subtype
	 * @return the message subtype
	 */
	public final byte getSubtype() {
		return m_subtype;
	}

	/**
	 * Get the status code (definable error, success,...)
	 * @return Status code.
	 */
	public final byte getStatusCode() {
		return m_statusCode;
	}

	/**
	 * Returns whether this message type allows parallel execution
	 * @return the exclusivity
	 */
	public final boolean isExclusive() {
		return m_exclusivity;
	}

	// Setters

	/**
	 * Set the status code (definable error, success,...)
	 * @param p_statusCode
	 *            the status code
	 */
	public final void setStatusCode(final byte p_statusCode) {
		m_statusCode = p_statusCode;
	}

	/**
	 * Sets source of the message
	 * @param p_source
	 *            the source node ID
	 */
	final void setSource(final short p_source) {
		m_source = p_source;
	}

	/**
	 * Sets destination of the message
	 * @param p_destination
	 *            the destination node ID
	 */
	final void setDestination(final short p_destination) {
		m_destination = p_destination;
	}

	// Methods
	/**
	 * Reads the message payload from the byte buffer
	 * @param p_buffer
	 *            the byte buffer
	 */
	protected void readPayload(final ByteBuffer p_buffer) {}

	/**
	 * Writes the message payload into the buffer
	 * @param p_buffer
	 *            the buffer
	 */
	protected void writePayload(final ByteBuffer p_buffer) {}

	/**
	 * Get the total number of bytes the payload requires to create a buffer.
	 * @return Number of bytes of the payload
	 */
	protected int getPayloadLengthForWrite() {
		return 0;
	}

	/**
	 * Get a ByteBuffer with the Message as content
	 * @return a ByteBuffer with the Message as content
	 */
	protected final ByteBuffer getBuffer() {
		int payloadSize;
		ByteBuffer buffer;

		payloadSize = getPayloadLengthForWrite();
		buffer = ByteBuffer.allocate(HEADER_SIZE + payloadSize);
		buffer = fillBuffer(buffer, payloadSize);
		buffer.flip();

		return buffer;
	}

	/**
	 * Fills a given ByteBuffer with the message
	 * @param p_buffer
	 *            a given ByteBuffer
	 * @param p_payloadSize
	 *            the payload size
	 * @return filled ByteBuffer
	 */
	private ByteBuffer fillBuffer(final ByteBuffer p_buffer, final int p_payloadSize) {
		// Put 3 byte message ID
		p_buffer.put((byte) (m_messageID >>> 16));
		p_buffer.put((byte) (m_messageID >>> 8));
		p_buffer.put((byte) m_messageID);

		p_buffer.put(m_type);
		p_buffer.put(m_subtype);
		if (m_exclusivity) {
			p_buffer.put((byte) 1);
		} else {
			p_buffer.put((byte) 0);
		}
		p_buffer.put(m_statusCode);
		p_buffer.putInt(p_payloadSize);

		writePayload(p_buffer);

		return p_buffer;
	}

	/**
	 * Get next free messageID
	 * @return next free messageID
	 */
	private static int getNextMessageID() {
		int ret;

		m_lock.lock();
		ret = m_nextMessageID++;
		m_lock.unlock();

		return ret;
	}

	/**
	 * Executed before a Message is send (not forwarded)
	 */
	protected void beforeSend() {}

	/**
	 * Executed after a Message is send (not forwarded)
	 */
	protected void afterSend() {}

	/**
	 * Creates a Message from the given byte buffer
	 * @param p_buffer
	 *            the byte buffer
	 * @param p_messageDirectory
	 *            the message directory
	 * @return the created Message
	 * @throws NetworkException
	 *             if the message header could not be created
	 */
	protected static AbstractMessage createMessageHeader(final ByteBuffer p_buffer, final MessageDirectory p_messageDirectory) throws NetworkException {
		AbstractMessage ret = null;
		int messageID;
		byte type;
		byte subtype;
		boolean exclusivity;
		byte statusCode;

		assert p_buffer != null;

		if (p_buffer.remaining() < HEADER_SIZE - BYTES_PAYLOAD_SIZE) {
			throw new NetworkException("Incomplete header");
		}

		messageID = ((p_buffer.get() & 0xFF) << 16) + ((p_buffer.get() & 0xFF) << 8) + (p_buffer.get() & 0xFF);
		type = p_buffer.get();
		subtype = p_buffer.get();
		exclusivity = p_buffer.get() == 1;
		statusCode = p_buffer.get();

		try {
			ret = p_messageDirectory.getInstance(type, subtype);
		} catch (final Exception e) {
			throw new NetworkException("Unable to create message of type " + type + ", subtype " + subtype, e);
		}

		ret.m_messageID = messageID;
		ret.m_type = type;
		ret.m_subtype = subtype;
		ret.m_exclusivity = exclusivity;
		ret.m_statusCode = statusCode;

		return ret;
	}

	@Override
	public final String toString() {
		return getClass().getSimpleName() + "[" + m_messageID + ", " + m_source + ", " + m_destination + "]";
	}

}
