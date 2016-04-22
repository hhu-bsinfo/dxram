
package de.hhu.bsinfo.dxcompute.ms.messages;

import de.hhu.bsinfo.menet.AbstractResponse;

/**
 * Response of the master to a join request by a slave.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class SlaveJoinResponse extends AbstractResponse {
	/**
	 * Creates an instance of SlaveJoinResponse.
	 * This constructor is used when receiving this message.
	 */
	public SlaveJoinResponse() {
		super();
	}

	/**
	 * Creates an instance of SlaveJoinResponse.
	 * This constructor is used when sending this message.
	 * @param p_request
	 *            request to respond to.
	 */
	public SlaveJoinResponse(final SlaveJoinRequest p_request) {
		super(p_request, MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_RESPONSE);
	}
}
