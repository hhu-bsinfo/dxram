package de.hhu.bsinfo.dxram.chunk.messages;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import de.hhu.bsinfo.menet.AbstractResponse;

public class GetLocalChunkIDRangesResponse extends AbstractResponse {
	private ArrayList<Long> m_chunkIDRanges = null;
	
	/**
	 * Creates an instance of StatusResponse.
	 * This constructor is used when receiving this message.
	 */
	public GetLocalChunkIDRangesResponse() {
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
	public GetLocalChunkIDRangesResponse(final GetLocalChunkIDRangesRequest p_request, final ArrayList<Long> p_chunkIDRanges) {
		super(p_request, ChunkMessages.SUBTYPE_GET_LOCAL_CHUNKID_RANGES_RESPONSE);

		m_chunkIDRanges = p_chunkIDRanges;
	}
	
	public ArrayList<Long> getChunkIDRanges() {
		return m_chunkIDRanges;
	}
	
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_chunkIDRanges.size());
		for (int i = 0; i < m_chunkIDRanges.size(); i++) {
			p_buffer.putLong(m_chunkIDRanges.get(0));
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_chunkIDRanges = new ArrayList<Long>(p_buffer.getInt());
		for (int i = 0; i < m_chunkIDRanges.size(); i++) {
			m_chunkIDRanges.add(p_buffer.getLong());
		}
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES + Long.BYTES * m_chunkIDRanges.size();
	}
}
