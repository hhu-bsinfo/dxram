package de.hhu.bsinfo.dxcompute.coord.messages;

import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Message to be sent to all available peers by the master
 * looking for further slaves.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 10.02.16
 */
public class MasterSyncBarrierBroadcastMessage extends AbstractMessage
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
	public MasterSyncBarrierBroadcastMessage(final short p_destination) {
		super(p_destination, CoordinatorMessages.TYPE, CoordinatorMessages.SUBTYPE_MASTER_SYNC_BARRIER_BROADCAST);
	}
}
