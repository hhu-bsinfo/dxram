
package de.uniduesseldorf.dxram.core.net;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.NetworkException;
import de.uniduesseldorf.dxram.core.util.NodeID;
import de.uniduesseldorf.dxram.utils.Contract;

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
	public static final boolean DEFAULT_EXCLUSIVITY_VALUE = false;
	public static final byte DEFAULT_RATING_VALUE = 1;

	public static final byte HEADER_SIZE = 10;

	// Attributes
	private int m_messageID;
	private short m_source;
	private short m_destination;
	private byte m_type;
	private byte m_subtype;
	private boolean m_exclusivity;

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
	}

	/**
	 * Creates an instance of Message
	 * @param p_destination
	 *            the destination
	 * @param p_type
	 *            the message type
	 */
	public AbstractMessage(final short p_destination, final byte p_type) {
		this(getNextMessageID(), p_destination, p_type, DEFAULT_SUBTYPE, DEFAULT_RATING_VALUE, DEFAULT_EXCLUSIVITY_VALUE);
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
		this(getNextMessageID(), p_destination, p_type, p_subtype, DEFAULT_RATING_VALUE, DEFAULT_EXCLUSIVITY_VALUE);
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
		this(getNextMessageID(), p_destination, p_type, p_subtype, DEFAULT_RATING_VALUE, p_exclusivity);
	}

	/**
	 * Creates an instance of Message
	 * @param p_destination
	 *            the destination
	 * @param p_type
	 *            the message type
	 * @param p_subtype
	 *            the message subtype
	 * @param p_ratingValue
	 *            the rating value of the message
	 */
	public AbstractMessage(final short p_destination, final byte p_type, final byte p_subtype, final byte p_ratingValue) {
		this(getNextMessageID(), p_destination, p_type, p_subtype, p_ratingValue, DEFAULT_EXCLUSIVITY_VALUE);
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
		this(p_messageID, p_destination, p_type, p_subtype, DEFAULT_RATING_VALUE, DEFAULT_EXCLUSIVITY_VALUE);
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
	 * @param p_ratingValue
	 *            the rating value of the message
	 * @param p_exclusivity
	 *            whether this message type allows parallel execution
	 */
	protected AbstractMessage(final int p_messageID, final short p_destination, final byte p_type,
			final byte p_subtype, final byte p_ratingValue, final boolean p_exclusivity) {
		NodeID.check(p_destination);

		m_messageID = p_messageID;
		m_source = NodeID.getLocalNodeID();
		m_destination = p_destination;
		m_type = p_type;
		m_subtype = p_subtype;
		m_exclusivity = p_exclusivity;
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
	 * Returns whether this message type allows parallel execution
	 * @return the exclusivity
	 */
	public final boolean isExclusive() {
		return m_exclusivity;
	}

	// Setters
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
	 * Get the total number of bytes the payload requires
	 * @return Number of bytes of the payload
	 */
	protected int getPayloadLength() {
		return 0;
	}

	/**
	 * Get a ByteBuffer with the Message as content
	 * @return a ByteBuffer with the Message as content
	 */
	protected final ByteBuffer getBuffer() {
		int payloadSize;
		ByteBuffer buffer;

		payloadSize = getPayloadLength();
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
	 * Send the Message using the given NetworkInterface
	 * @param p_network
	 *            the NetworkInterface
	 * @throws NetworkException
	 *             if the message could not be send
	 */
	public final void send(final NetworkInterface p_network) throws NetworkException {
		Contract.checkNotNull(p_network, "no network given");

		beforeSend();

		p_network.sendMessage(this);

		afterSend();
	}

	/**
	 * Forward the Message to the given destination using the given NetworkInterface
	 * @param p_destination
	 *            the destination
	 * @param p_network
	 *            the NetworkInterface
	 * @throws DXRAMException
	 *             if the message could not be forwarded
	 */
	public final void forward(final short p_destination, final NetworkInterface p_network) throws DXRAMException {
		NodeID.check(p_destination);
		Contract.checkNotNull(p_network, "no network given");

		m_destination = p_destination;

		// LOGGER.trace("forwarding message to " + p_destination);

		p_network.sendMessage(this);
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
	 * @return the created Message
	 * @throws NetworkException
	 *             if the message header could not be created
	 */
	protected static AbstractMessage createMessageHeader(final ByteBuffer p_buffer) throws NetworkException {
		AbstractMessage ret = null;
		int messageID;
		byte type;
		byte subtype;
		boolean exclusivity;

		Contract.checkNotNull(p_buffer, "no bytes given");

		if (p_buffer.remaining() < HEADER_SIZE - BYTES_PAYLOAD_SIZE) {
			throw new NetworkException("Incomplete header");
		}

		messageID = ((p_buffer.get() & 0xFF) << 16) + ((p_buffer.get() & 0xFF) << 8) + (p_buffer.get() & 0xFF);
		type = p_buffer.get();
		subtype = p_buffer.get();
		exclusivity = p_buffer.get() == 1;

		try {
			ret = MessageDirectory.getInstance(type, subtype);
		} catch (final Exception e) {
			throw new NetworkException("Unable to create message", e);
		}

		ret.m_messageID = messageID;
		ret.m_type = type;
		ret.m_subtype = subtype;
		ret.m_exclusivity = exclusivity;

		return ret;
	}

	/**
	 * Creates a string representation of the message
	 * @return the string representation
	 */
	public final String print() {
		return getClass().getSimpleName() + "[" + m_messageID + ", " + m_source + ", " + m_destination + "]";
	}

}
