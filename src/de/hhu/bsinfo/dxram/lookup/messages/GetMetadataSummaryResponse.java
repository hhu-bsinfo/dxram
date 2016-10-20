
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to a GetMetadataSummaryRequest
 * @author Kevin Beineke 12.10.2016
 */
public class GetMetadataSummaryResponse extends AbstractResponse {

	// Attributes
	private String m_summary;

	// Constructors
	/**
	 * Creates an instance of GetMetadataSummaryResponse
	 */
	public GetMetadataSummaryResponse() {
		super();
	}

	/**
	 * Creates an instance of SendBackupsMessage
	 * @param p_request
	 *            the corresponding GetMetadataSummaryRequest
	 * @param p_summary
	 *            the metadata summary
	 */
	public GetMetadataSummaryResponse(final GetMetadataSummaryRequest p_request, final String p_summary) {
		super(p_request, LookupMessages.SUBTYPE_GET_METADATA_SUMMARY_RESPONSE);

		m_summary = p_summary;
	}

	// Getters
	/**
	 * Get metadata summary
	 * @return the metadata summary
	 */
	public final String getMetadataSummary() {
		return m_summary;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_summary.getBytes().length);
		p_buffer.put(m_summary.getBytes());
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		int length;
		byte[] data;

		length = p_buffer.getInt();
		data = new byte[length];
		p_buffer.get(data);
		m_summary = new String(data);
	}

	@Override
	protected final int getPayloadLength() {
		return m_summary.getBytes().length + Integer.BYTES;
	}

}
