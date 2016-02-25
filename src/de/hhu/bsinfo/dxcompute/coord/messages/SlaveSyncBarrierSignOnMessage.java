package de.hhu.bsinfo.dxcompute.coord.messages;

/**
 * Response by a slave if he is looking to sync with a master barrier.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 10.02.16
 */
public class SlaveSyncBarrierSignOnMessage extends SyncMessage {
	/**
	 * Creates an instance of SlaveSyncBarrierSignOnMessage.
	 * This constructor is used when receiving this message.
	 */
	public SlaveSyncBarrierSignOnMessage() {
		super();
	}

	/**
	 * Creates an instance of SlaveSyncBarrierSignOnMessage.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 * @param p_syncToken Token to correctly identify responses to a sync message
	 */
	public SlaveSyncBarrierSignOnMessage(final short p_destination, final int p_syncToken) {
		super(p_destination, CoordinatorMessages.SUBTYPE_SLAVE_SYNC_BARRIER_SIGN_ON, p_syncToken);
	}
}
