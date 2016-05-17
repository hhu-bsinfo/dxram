package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.menet.AbstractRequest;

import java.nio.ByteBuffer;

/**
 * Request to allocate memory in the superpeer storage.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 17.05.15
 */
public class SuperpeerStorageCreateRequest extends AbstractRequest {
	private short m_superpeerNodeId;
	private int m_size;

	/**
	 * Creates an instance of SuperpeerStorageCreateRequest
	 */
	public SuperpeerStorageCreateRequest() {
		super();
	}

	/**
	 * Creates an instance of SuperpeerStorageCreateRequest
	 *
	 * @param p_destination     the destination
	 * @param p_superpeerNodeId Node id of the superpeer storage.
	 * @param p_size            Size in bytes of the data to store
	 */
	public SuperpeerStorageCreateRequest(final short p_destination, final short p_superpeerNodeId, final int p_size) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_CREATE_REQUEST);

		m_superpeerNodeId = p_superpeerNodeId;
		m_size = p_size;
	}

	/**
	 * Get the node id of the superpeer.
	 *
	 * @return Superpeer node id.
	 */
	public short getSuperpeerNodeId() {
		return m_superpeerNodeId;
	}

	/**
	 * Get the size for the allocation
	 *
	 * @return Size in bytes
	 */
	public int getSize() {
		return m_size;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putShort(m_superpeerNodeId);
		p_buffer.putInt(m_size);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_superpeerNodeId = p_buffer.getShort();
		m_size = p_buffer.getInt();
	}

	@Override
	protected final int getPayloadLength() {
		return Short.BYTES + Integer.BYTES;
	}
}
