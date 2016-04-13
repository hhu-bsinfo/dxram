
package de.hhu.bsinfo.dxcompute.coord.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractResponse;

public class BarrierSlaveSignOnResponse extends AbstractResponse {
	private int m_syncToken = -1;

	/**
	 * Creates an instance of SlaveSyncBarrierSignOnMessage.
	 * This constructor is used when receiving this message.
	 */
	public BarrierSlaveSignOnResponse() {
		super();
	}

	/**
	 * Creates an instance of SlaveSyncBarrierSignOnMessage.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 * @param p_syncToken
	 *            Token to correctly identify responses to a sync message
	 * @param p_data
	 *            Some custom data to pass along.
	 */
	public BarrierSlaveSignOnResponse(final BarrierSlaveSignOnRequest p_request, final int p_syncToken) {
		super(p_request, CoordinatorMessages.SUBTYPE_BARRIER_SLAVE_SIGN_ON_RESPONSE);

		m_syncToken = p_syncToken;
	}

	/**
	 * Get the sync token.
	 * @return Sync token.
	 */
	public int getSyncToken() {
		return m_syncToken;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_syncToken);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_syncToken = p_buffer.getInt();
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES;
	}
}
