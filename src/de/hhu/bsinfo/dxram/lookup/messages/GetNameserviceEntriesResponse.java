
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to a GetMappingCountRequest
 * @author klein 26.03.2015
 */
public class GetNameserviceEntriesResponse extends AbstractResponse {

	private byte[] m_entries;

	// Constructors
	/**
	 * Creates an instance of GetNameserviceEntriesResponse
	 */
	public GetNameserviceEntriesResponse() {
		super();
	}

	/**
	 * Creates an instance of GetNameserviceEntriesResponse
	 * @param p_request
	 *            the request
	 * @param p_entries
	 *            the count
	 */
	public GetNameserviceEntriesResponse(final GetNameserviceEntriesRequest p_request,
			final byte[] p_entries) {
		super(p_request, LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRIES_RESPONSE);

		m_entries = p_entries;
	}

	// Getters
	/**
	 * Get the entries.
	 * @return Entries
	 */
	public byte[] getEntries() {
		return m_entries;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		if (m_entries.length == 0) {
			p_buffer.putInt(0);
		} else {
			p_buffer.putInt(m_entries.length);
			p_buffer.put(m_entries);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		int length = p_buffer.getInt();
		if (length != 0) {
			m_entries = new byte[length];
			p_buffer.get(m_entries);
		}
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES + m_entries.length;
	}

}
