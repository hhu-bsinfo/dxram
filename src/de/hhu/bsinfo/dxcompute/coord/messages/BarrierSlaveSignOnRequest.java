
package de.hhu.bsinfo.dxcompute.coord.messages;

import de.hhu.bsinfo.menet.AbstractRequest;

import java.nio.ByteBuffer;

/**
 * Request sent when a slave hits a barrier and wants to sign on (i.e. notify the master).
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 */
public class BarrierSlaveSignOnRequest extends AbstractRequest {
	private int m_syncToken = -1;
	private long m_data = -1;

	/**
	 * Creates an instance of SlaveSyncBarrierSignOnMessage.
	 * This constructor is used when receiving this message.
	 */
	public BarrierSlaveSignOnRequest() {
		super();
	}

	/**
	 * Creates an instance of SlaveSyncBarrierSignOnMessage.
	 * This constructor is used when sending this message.
	 *
	 * @param p_destination the destination node id.
	 * @param p_syncToken   Token to correctly identify responses to a sync message
	 * @param p_data        Some custom data to pass along.
	 */
	public BarrierSlaveSignOnRequest(final short p_destination, final int p_syncToken, final long p_data) {
		super(p_destination, CoordinatorMessages.TYPE, CoordinatorMessages.SUBTYPE_BARRIER_SLAVE_SIGN_ON_REQUEST);

		m_syncToken = p_syncToken;
		m_data = p_data;
	}

	/**
	 * Get the sync token.
	 *
	 * @return Sync token.
	 */
	public int getSyncToken() {
		return m_syncToken;
	}

	/**
	 * Get the custom data passed along with the request.
	 *
	 * @return Data.
	 */
	public long getData() {
		return m_data;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_syncToken);
		p_buffer.putLong(m_data);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_syncToken = p_buffer.getInt();
		m_data = p_buffer.getLong();
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES + Long.BYTES;
	}
}
