package de.hhu.bsinfo.dxcompute.coord.messages;

/**
 * Message to be sent to all available peers by the master
 * looking for further slaves.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 10.02.16
 */
public class MasterSyncBarrierBroadcastMessage extends SyncMessage
{	
	/**
	 * Creates an instance of MasterSyncBarrierBroadcastMessage.
	 * This constructor is used when receiving this message.
	 */
	public MasterSyncBarrierBroadcastMessage() {
		super();
	}

	/**
	 * Creates an instance of MasterSyncBarrierBroadcastMessage.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 */
	public MasterSyncBarrierBroadcastMessage(final short p_destination, final int p_syncToken, final long p_data) {
		super(p_destination, CoordinatorMessages.SUBTYPE_MASTER_SYNC_BARRIER_BROADCAST, p_syncToken, p_data);
	}
}
