
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import de.hhu.bsinfo.menet.AbstractResponse;
import de.hhu.bsinfo.utils.Pair;

/**
 * Response to a GetMappingCountRequest
 * @author klein 26.03.2015
 */
public class GetNameserviceEntriesResponse extends AbstractResponse {

	private ArrayList<Pair<Integer, Long>> m_entries;

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
			final ArrayList<Pair<Integer, Long>> p_entries) {
		super(p_request, LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRIES_RESPONSE);

		m_entries = p_entries;
	}

	// Getters
	/**
	 * Get the entries.
	 * @return Entries
	 */
	public ArrayList<Pair<Integer, Long>> getEntries() {
		return m_entries;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_entries.size());
		for (Pair<Integer, Long> entry : m_entries) {
			p_buffer.putInt(entry.first());
			p_buffer.putLong(entry.second());
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		int elems = p_buffer.getInt();
		m_entries = new ArrayList<Pair<Integer, Long>>(elems);
		for (int i = 0; i < elems; i++) {
			Pair<Integer, Long> pair = new Pair<Integer, Long>();
			pair.m_first = p_buffer.getInt();
			pair.m_second = p_buffer.getLong();
			m_entries.add(pair);
		}
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES + (Integer.BYTES + Long.BYTES) * m_entries.size();
	}

}
