
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Message to free an allocation item on the superpeer storage.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 17.05.15
 */
public class SuperpeerStorageRemoveMessage extends AbstractMessage {
	private int m_storageId;
	private boolean m_replicate;

	/**
	 * Creates an instance of SuperpeerStorageRemoveMessage
	 */
	public SuperpeerStorageRemoveMessage() {
		super();
	}

	/**
	 * Creates an instance of SuperpeerStorageRemoveMessage
	 * @param p_destination
	 *            the destination
	 * @param p_storageId
	 *            Storage id of an allocated block of memory on the superpeer.
	 * @param p_replicate
	 *            True if replicate message, false if not
	 */
	public SuperpeerStorageRemoveMessage(final short p_destination, final int p_storageId, final boolean p_replicate) {
		super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
				LookupMessages.SUBTYPE_SUPERPEER_STORAGE_REMOVE_MESSAGE);

		m_storageId = p_storageId;
		m_replicate = p_replicate;
	}

	/**
	 * Get the storage id to free on the current superpeer.
	 * @return Storage id to free.
	 */
	public int getStorageId() {
		return m_storageId;
	}

	/**
	 * Check if this request is a replicate message.
	 * @return True if replicate message.
	 */
	public boolean isReplicate() {
		return m_replicate;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_storageId);
		p_buffer.put((byte) (m_replicate ? 1 : 0));
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_storageId = p_buffer.getInt();
		m_replicate = p_buffer.get() != 0;
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES + Byte.BYTES;
	}
}
