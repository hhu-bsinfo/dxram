
package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.menet.AbstractResponse;

/**
 * Response to free request.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.05.16
 */
public class BarrierFreeResponse extends AbstractResponse {
	/**
	 * Creates an instance of BarrierFreeRequest
	 */
	public BarrierFreeResponse() {
		super();
	}

	/**
	 * Creates an instance of BarrierFreeRequest
	 * @param p_request
	 *            The request to respond to
	 */
	public BarrierFreeResponse(final BarrierFreeRequest p_request) {
		super(p_request, LookupMessages.SUBTYPE_BARRIER_FREE_RESPONSE);
	}
}
