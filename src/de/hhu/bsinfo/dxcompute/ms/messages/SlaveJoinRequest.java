
package de.hhu.bsinfo.dxcompute.ms.messages;

import de.hhu.bsinfo.menet.AbstractRequest;

public class SlaveJoinRequest extends AbstractRequest {
	/**
	 * Creates an instance of SlaveSyncBarrierSignOnMessage.
	 * This constructor is used when receiving this message.
	 */
	public SlaveJoinRequest() {
		super();
	}

	/**
	 * Creates an instance of SlaveSyncBarrierSignOnMessage.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 */
	public SlaveJoinRequest(final short p_destination) {
		super(p_destination, MasterSlaveMessages.TYPE, MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_REQUEST);
	}
}
