package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractResponse;

/**
 * Response to a GetMappingCountRequest
 * @author klein 26.03.2015
 */
public class GetNameserviceEntryCountResponse extends AbstractResponse {

	// Attributes
	private long m_count;

	// Constructors
	/**
	 * Creates an instance of GetMappingCountResponse
	 */
	public GetNameserviceEntryCountResponse() {
		super();

		m_count = 0;
	}

	/**
	 * Creates an instance of GetMappingCountResponse
	 * @param p_request
	 *            the request
	 * @param p_count
	 *            the count
	 */
	public GetNameserviceEntryCountResponse(final GetNameserviceEntryCountRequest p_request, final long p_count) {
		super(p_request, LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRY_COUNT_RESPONSE);

		m_count = p_count;
	}

	// Getters
	/**
	 * Get the count
	 * @return the count
	 */
	public final long getCount() {
		return m_count;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putLong(m_count);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_count = p_buffer.getLong();
	}

	@Override
	protected final int getPayloadLength() {
		return Long.BYTES;
	}

}
