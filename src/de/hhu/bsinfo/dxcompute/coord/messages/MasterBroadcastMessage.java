package de.hhu.bsinfo.dxcompute.coord.messages;

import de.hhu.bsinfo.menet.AbstractMessage;

public class MasterBroadcastMessage extends AbstractMessage
{	
	/**
	 * Creates an instance of MasterSyncBarrierBroadcastMessage.
	 * This constructor is used when receiving this message.
	 */
	public MasterBroadcastMessage() {
		super();
	}

	/**
	 * Creates an instance of MasterSyncBarrierBroadcastMessage.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 */
	public MasterBroadcastMessage(final short p_destination) {
		super(p_destination, CoordinatorMessages.TYPE, CoordinatorMessages.SUBTYPE_MASTER_BROADCAST_MESSAGE);
	}
}
