package de.hhu.bsinfo.dxgraph.coord.messages;

import de.hhu.bsinfo.menet.AbstractMessage;

public class MasterSlaveSyncBarrierMessage extends AbstractMessage {
	/**
	 * Creates an instance of SlaveSyncMessage.
	 * This constructor is used when receiving this message.
	 */
	public MasterSlaveSyncBarrierMessage() {
		super();
	}

	/**
	 * Creates an instance of SlaveSyncMessage.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 */
	public MasterSlaveSyncBarrierMessage(final short p_destination) {
		super(p_destination, CoordinatorMessages.TYPE, CoordinatorMessages.SUBTYPE_MASTER_SLAVE_SYNC_BARRIER);
	}
}
