
package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.menet.AbstractRequest;

public class RequestResponsibleSuperPeer extends AbstractRequest {

	/**
	 * Created because compiler
	 */
	public RequestResponsibleSuperPeer() {
		super();
	}

	/**
	 * Creates an instance of LogMessage
	 */
	public RequestResponsibleSuperPeer(final short p_destination) {

		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_REQUEST_RESPONSIBLE_SUPERPEER);

	}

}
