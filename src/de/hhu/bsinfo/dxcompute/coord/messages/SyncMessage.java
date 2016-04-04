package de.hhu.bsinfo.dxcompute.coord.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Base class for any kind of sync message used for coordination.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 10.02.16
 */
public class SyncMessage extends AbstractMessage
{
	private int m_syncToken = -1;
	private long m_data = -1;
	
	/**
	 * Creates an instance of MasterSyncBarrierBroadcastMessage.
	 * This constructor is used when receiving this message.
	 */
	public SyncMessage() {
		super();
	}

	/**
	 * Creates an instance of MasterSyncBarrierBroadcastMessage.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 * @param p_subtype Message subtype
	 * @param p_syncToken Token to correctly identify responses to a sync message
	 * @param p_data Some custom data.
	 */
	public SyncMessage(final short p_destination, final byte p_subtype, final int p_syncToken, final long p_data) {
		super(p_destination, CoordinatorMessages.TYPE, p_subtype);
	
		m_syncToken = p_syncToken;
		m_data = p_data;
	}
	
	/**
	 * Get the sync token.
	 * @return Sync token.
	 */
	public int getSyncToken() {
		return m_syncToken;
	}
	
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
	protected final int getPayloadLengthForWrite() {
		return Integer.BYTES + Long.BYTES;
	}
}
