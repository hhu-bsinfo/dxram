
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.menet.AbstractResponse;

/**
 * Response to a LookupRequest
 * @author Kevin Beineke
 *         06.09.2012
 */
public class GetLookupRangeResponse extends AbstractResponse {

	// Attributes
	private LookupRange m_lookupRange;

	// Constructors
	/**
	 * Creates an instance of LookupResponse
	 */
	public GetLookupRangeResponse() {
		super();

		m_lookupRange = null;
	}

	/**
	 * Creates an instance of LookupResponse
	 * @param p_request
	 *            the corresponding LookupRequest
	 * @param p_lookupRange
	 *            the primary peer, backup peers and range
	 */
	public GetLookupRangeResponse(final GetLookupRangeRequest p_request, final LookupRange p_lookupRange) {
		super(p_request, LookupMessages.SUBTYPE_GET_LOOKUP_RANGE_RESPONSE);

		m_lookupRange = p_lookupRange;
	}

	// Getters
	/**
	 * Get lookupRange
	 * @return the LookupRange
	 */
	public final LookupRange getLookupRange() {
		return m_lookupRange;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		if (m_lookupRange == null) {
			p_buffer.put((byte) 0);
		} else {
			final MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);
			exporter.setPayloadSize(m_lookupRange.sizeofObject());

			p_buffer.put((byte) 1);
			exporter.exportObject(m_lookupRange);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		if (p_buffer.get() != 0) {
			final MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);

			m_lookupRange = new LookupRange();
			importer.setPayloadSize(m_lookupRange.sizeofObject());
			importer.importObject(m_lookupRange);
		}
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		int ret;

		if (m_lookupRange == null) {
			ret = Byte.BYTES;
		} else {
			ret = Byte.BYTES + m_lookupRange.sizeofObject();
		}

		return ret;
	}

}
