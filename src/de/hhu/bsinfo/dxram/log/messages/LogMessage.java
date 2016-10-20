
package de.hhu.bsinfo.dxram.log.messages;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Message for logging a Chunk on a remote node
 * @author Kevin Beineke 20.04.2014
 */
public class LogMessage extends AbstractMessage {

	// Attributes
	private byte m_rangeID;
	private DataStructure[] m_dataStructures;
	private ByteBuffer m_buffer;

	// Constructors
	/**
	 * Creates an instance of LogMessage
	 */
	public LogMessage() {
		super();

		m_rangeID = -1;
		m_dataStructures = null;
		m_buffer = null;
	}

	/**
	 * Creates an instance of LogMessage
	 * @param p_destination
	 *            the destination
	 * @param p_dataStructures
	 *            the data structures to store
	 */
	public LogMessage(final short p_destination, final DataStructure... p_dataStructures) {
		super(p_destination, DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_LOG_MESSAGE, true);

		m_rangeID = -1;
		m_dataStructures = p_dataStructures;
	}

	/**
	 * Creates an instance of LogMessage
	 * @param p_destination
	 *            the destination
	 * @param p_dataStructures
	 *            the data structures to store
	 * @param p_rangeID
	 *            the RangeID
	 */
	public LogMessage(final short p_destination, final byte p_rangeID, final DataStructure... p_dataStructures) {
		super(p_destination, DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_LOG_MESSAGE, true);

		m_rangeID = p_rangeID;
		m_dataStructures = p_dataStructures;
	}

	// Getters
	/**
	 * Get the message buffer
	 * @return the message buffer
	 */
	public final ByteBuffer getMessageBuffer() {
		return m_buffer;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.put(m_rangeID);

		p_buffer.putInt(m_dataStructures.length);
		final MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);
		for (DataStructure dataStructure : m_dataStructures) {
			final int size = dataStructure.sizeofObject();

			p_buffer.putLong(dataStructure.getID());
			exporter.setPayloadSize(size);
			p_buffer.putInt(size);
			p_buffer.order(ByteOrder.nativeOrder());
			exporter.exportObject(dataStructure);
			p_buffer.order(ByteOrder.BIG_ENDIAN);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_buffer = p_buffer;
	}

	@Override
	protected final int getPayloadLength() {
		if (m_dataStructures != null) {
			int ret = Byte.BYTES + Integer.BYTES;

			for (DataStructure dataStructure : m_dataStructures) {
				ret += Long.BYTES + Integer.BYTES + dataStructure.sizeofObject();
			}

			return ret;
		} else {
			return 0;
		}
	}
}
