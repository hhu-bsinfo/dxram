
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.SuperpeerStorage;
import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to the status request.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.05.15
 */
public class SuperpeerStorageStatusResponse extends AbstractResponse {
	private SuperpeerStorage.Status m_status;

	/**
	 * Creates an instance of SuperpeerStorageCreateRequest
	 */
	public SuperpeerStorageStatusResponse() {
		super();
	}

	/**
	 * Creates an instance of SuperpeerStorageCreateRequest
	 * @param p_request
	 *            Request to respond to
	 * @param p_status
	 *            Status to send with the response
	 */
	public SuperpeerStorageStatusResponse(final SuperpeerStorageStatusRequest p_request,
			final SuperpeerStorage.Status p_status) {
		super(p_request, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_STATUS_RESPONSE);

		m_status = p_status;
	}

	public SuperpeerStorage.Status getStatus() {
		return m_status;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);
		exporter.exportObject(m_status);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);
		m_status = new SuperpeerStorage.Status();
		importer.importObject(m_status);
	}

	@Override
	protected final int getPayloadLength() {
		return m_status.sizeofObject();
	}
}
