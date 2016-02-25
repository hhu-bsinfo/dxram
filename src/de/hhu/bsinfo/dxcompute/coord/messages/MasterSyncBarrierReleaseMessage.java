package de.hhu.bsinfo.dxcompute.coord.messages;

/**
 * Message by master to send to all slaves signed on to his barrier,
 * if enough slaves have signed on.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 10.02.16
 */
public class MasterSyncBarrierReleaseMessage extends SyncMessage {
	/**
	 * Creates an instance of MasterSyncBarrierReleaseMessage.
	 * This constructor is used when receiving this message.
	 */
	public MasterSyncBarrierReleaseMessage() {
		super();
	}

	/**
	 * Creates an instance of MasterSyncBarrierReleaseMessage.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 * @param p_syncToken Token to correctly identify responses to a sync message
	 */
	public MasterSyncBarrierReleaseMessage(final short p_destination, final int p_syncToken) {
		super(p_destination, CoordinatorMessages.SUBTYPE_MASTER_SYNC_BARRIER_RELEASE, p_syncToken);
	}
}