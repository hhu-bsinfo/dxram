package de.hhu.bsinfo.dxgraph.coord.messages;

import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Message by master to send to all slaves signed on to his barrier,
 * if enough slaves have signed on.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 10.02.16
 */
public class MasterSyncBarrierReleaseMessage extends AbstractMessage {
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
	 */
	public MasterSyncBarrierReleaseMessage(final short p_destination) {
		super(p_destination, CoordinatorMessages.TYPE, CoordinatorMessages.SUBTYPE_MASTER_SYNC_BARRIER_RELEASE);
	}
}