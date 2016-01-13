package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.dxram.lookup.Locations;
import de.hhu.bsinfo.menet.AbstractResponse;

/**
 * Response to a LookupRequest
 * @author Kevin Beineke
 *         06.09.2012
 */
public class LookupResponse extends AbstractResponse {

	// Attributes
	private Locations m_locations;

	// Constructors
	/**
	 * Creates an instance of LookupResponse
	 */
	public LookupResponse() {
		super();

		m_locations = null;
	}

	/**
	 * Creates an instance of LookupResponse
	 * @param p_request
	 *            the corresponding LookupRequest
	 * @param p_locations
	 *            the primary peer, backup peers and range
	 */
	public LookupResponse(final LookupRequest p_request, final Locations p_locations) {
		super(p_request, LookupMessages.SUBTYPE_LOOKUP_RESPONSE);

		m_locations = p_locations;
	}

	// Getters
	/**
	 * Get locations
	 * @return the locations
	 */
	public final Locations getLocations() {
		return m_locations;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		if (m_locations == null) {
			p_buffer.put((byte) 0);
		} else {
			MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);
			exporter.setPayloadSize(m_locations.sizeofObject());
			
			p_buffer.put((byte) 1);
			exporter.exportObject(m_locations);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		if (p_buffer.get() != 0) {
			MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);
			
			m_locations = new Locations();
			importer.setPayloadSize(m_locations.sizeofObject());
			importer.importObject(m_locations);
		}
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		int ret;

		if (m_locations == null) {
			ret = Byte.BYTES;
		} else {
			ret = Byte.BYTES + m_locations.sizeofObject();
		}

		return ret;
	}

}
