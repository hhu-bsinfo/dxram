
package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to the change size request
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 06.05.16
 */
public class BarrierChangeSizeResponse extends AbstractResponse {
	/**
	 * Creates an instance of BarrierChangeSizeResponse
	 */
	public BarrierChangeSizeResponse() {
		super();

	}

	/**
	 * Creates an instance of BarrierChangeSizeReBarrierChangeSizeResponsesponse
	 * @param p_request
	 *            the corresponding BarrierAllocRequest
	 */
	public BarrierChangeSizeResponse(final BarrierChangeSizeRequest p_request) {
		super(p_request, LookupMessages.SUBTYPE_BARRIER_CHANGE_SIZE_RESPONSE);
	}
}
