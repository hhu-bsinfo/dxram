package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.menet.AbstractResponse;

import java.nio.ByteBuffer;

/**
 * Response to the create request.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 17.05.15
 */
public class SuperpeerStorageCreateResponse extends AbstractResponse {
	private int m_storageId;

	/**
	 * Creates an instance of SuperpeerStorageCreateResponse
	 */
	public SuperpeerStorageCreateResponse() {
		super();
	}

	/**
	 * Creates an instance of SuperpeerStorageCreateResponse
	 *
	 * @param p_request   The request to respond to
	 * @param p_storageId Id of the allocated storage location on the superpeer.
	 */
	public SuperpeerStorageCreateResponse(final SuperpeerStorageCreateRequest p_request, final int p_storageId) {
		super(p_request, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_CREATE_RESPONSE);

		m_storageId = p_storageId;
	}

	/**
	 * Get the allocated storage id.
	 *
	 * @return Storage id on the superpeer of this message or -1 for alloc failure.
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
