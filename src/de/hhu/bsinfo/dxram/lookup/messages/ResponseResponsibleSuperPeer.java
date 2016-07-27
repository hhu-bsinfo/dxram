
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractResponse;

public class ResponseResponsibleSuperPeer extends AbstractResponse {

	private short m_superPeer;

	/**
	 * Created because compiler
	 */
	public ResponseResponsibleSuperPeer() {
		super();
	}

	/**
	 * Creates an instance of LogMessage
	 */
	public ResponseResponsibleSuperPeer(final RequestResponsibleSuperPeer p_request, final short p_superPeer) {

		super(p_request, LookupMessages.SUBTYPE_RESPONSE_RESPONSIBLE_SUPERPEER);
		m_superPeer = p_superPeer;
	}

	public short getResponsibleSuperPeer() {
		return m_superPeer;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {

		p_buffer.putShort(m_superPeer);

	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {

		m_superPeer = p_buffer.getShort();

	}

	@Override
	protected final int getPayloadLength() {

		return Short.BYTES;
	}

}
