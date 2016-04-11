
package de.hhu.bsinfo.dxcompute.ms.messages;

import de.hhu.bsinfo.menet.AbstractResponse;

public class SlaveJoinResponse extends AbstractResponse {
	/**
	 * Creates an instance of SlaveSyncBarrierSignOnMessage.
	 * This constructor is used when receiving this message.
	 */
	public SlaveJoinResponse() {
		super();
	}

	/**
	 * Creates an instance of SlaveSyncBarrierSignOnMessage.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 */
	public SlaveJoinResponse(final SlaveJoinRequest p_request) {
		super(p_request, MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_RESPONSE);
	}
}
