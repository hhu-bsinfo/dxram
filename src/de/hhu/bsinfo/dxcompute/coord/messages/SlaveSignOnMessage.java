package de.hhu.bsinfo.dxcompute.coord.messages;

import de.hhu.bsinfo.menet.AbstractMessage;

public class SlaveSignOnMessage extends AbstractMessage {
	/**
	 * Creates an instance of SlaveSyncBarrierSignOnMessage.
	 * This constructor is used when receiving this message.
	 */
	public SlaveSignOnMessage() {
		super();
	}

	/**
	 * Creates an instance of SlaveSyncBarrierSignOnMessage.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 * @param p_syncToken Token to correctly identify responses to a sync message
	 * @param p_data Some custom data to pass along.
	 */
	public SlaveSignOnMessage(final short p_destination) {
		super(p_destination, CoordinatorMessages.TYPE, CoordinatorMessages.SUBTYPE_SLAVE_SIGN_ON_MESSAGE);
	}
}
