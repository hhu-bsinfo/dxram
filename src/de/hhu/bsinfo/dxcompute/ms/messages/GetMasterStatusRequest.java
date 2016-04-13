
package de.hhu.bsinfo.dxcompute.ms.messages;

import de.hhu.bsinfo.menet.AbstractRequest;

public class GetMasterStatusRequest extends AbstractRequest {
	/**
	 * Creates an instance of GetListOfSlavesRequest.
	 * This constructor is used when receiving this message.
	 */
	public GetMasterStatusRequest() {
		super();
	}

	/**
	 * Creates an instance of GetListOfSlavesRequest.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 */
	public GetMasterStatusRequest(final short p_destination) {
		super(p_destination, MasterSlaveMessages.TYPE, MasterSlaveMessages.SUBTYPE_GET_MASTER_STATUS_REQUEST);
	}
}
