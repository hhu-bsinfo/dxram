
package de.hhu.bsinfo.dxram.chunk.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.menet.AbstractResponse;

/**
 * Response with status information about a remote chunk service.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 07.04.16
 */
public class StatusResponse extends AbstractResponse {

	private ChunkService.Status m_status;

	/**
	 * Creates an instance of StatusResponse.
	 * This constructor is used when receiving this message.
	 */
	public StatusResponse() {
		super();
	}

	/**
	 * Creates an instance of StatusResponse.
	 * This constructor is used when sending this message.
	 * @param p_request
	 *            the corresponding StatusRequest
	 * @param p_status
	 *            the requested Status
	 */
	public StatusResponse(final StatusRequest p_request, final ChunkService.Status p_status) {
		super(p_request, ChunkMessages.SUBTYPE_STATUS_RESPONSE);

		m_status = p_status;
	}

	/**
	 * Get the chunk service status.
	 * @return Chunk service status.
	 */
	public final ChunkService.Status getStatus() {
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
		importer.importObject(m_status);
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return m_status.sizeofObject();
	}
}
