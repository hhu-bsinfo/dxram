package de.hhu.bsinfo.dxgraph.coord.messages;

import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Response by a slave if he is looking to sync with a master barrier.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 10.02.16
 */
public class SlaveSyncBarrierSignOnMessage extends AbstractMessage {
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
	 */
	public SlaveSyncBarrierSignOnMessage(final short p_destination) {
		super(p_destination, CoordinatorMessages.TYPE, CoordinatorMessages.SUBTYPE_SLAVE_SYNC_BARRIER_SIGN_ON);
	}
}
