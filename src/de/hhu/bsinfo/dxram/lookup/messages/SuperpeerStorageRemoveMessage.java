package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.menet.AbstractMessage;

import java.nio.ByteBuffer;

/**
 * Message to free an allocation item on the superpeer storage.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 17.05.15
 */
public class SuperpeerStorageRemoveMessage extends AbstractMessage {
	private int m_storageId;

	/**
	 * Creates an instance of SuperpeerStorageRemoveMessage
	 */
	public SuperpeerStorageRemoveMessage() {
		super();
	}

	/**
	 * Creates an instance of SuperpeerStorageRemoveMessage
	 *
	 * @param p_destination the destination
	 * @param p_storageId   Storage id of an allocated block of memory on the superpeer.
	 */
	public SuperpeerStorageRemoveMessage(final short p_destination, final int p_storageId) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_REMOVE_MESSAGE);

		m_storageId = p_storageId;
	}

	/**
	 * Get the storage id to free on the current superpeer.
	 *
	 * @return Storage id to free.
	 */
	public int getStorageId() {
		return m_storageId;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_storageId);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_storageId = p_buffer.getInt();
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES;
	}
}
