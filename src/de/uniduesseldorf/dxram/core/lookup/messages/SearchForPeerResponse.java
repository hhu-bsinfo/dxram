package de.uniduesseldorf.dxram.core.lookup.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.menet.AbstractResponse;

/**
 * Response to a SearchForPeerRequest
 * @author Kevin Beineke
 *         06.09.2012
 */
public class SearchForPeerResponse extends AbstractResponse {

	// Attributes
	private short m_peer;

	// Constructors
	/**
	 * Creates an instance of SearchForPeerResponse
	 */
	public SearchForPeerResponse() {
		super();

		m_peer = -1;
	}

	/**
	 * Creates an instance of SearchForPeerResponse
	 * @param p_request
	 *            the corresponding SearchForPeerRequest
	 * @param p_peer
	 *            the peer that can be promoted
	 */
	public SearchForPeerResponse(final SearchForPeerRequest p_request, final short p_peer) {
		super(p_request, LookupMessages.SUBTYPE_SEARCH_FOR_PEER_RESPONSE);

		m_peer = p_peer;
	}

	// Getters
	/**
	 * Get peer
	 * @return the NodeID
	 */
	public final short getPeer() {
		return m_peer;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putShort(m_peer);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_peer = p_buffer.getShort();
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Short.BYTES;
	}

}
