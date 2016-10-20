
package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to a NotifyAboutFailedPeerRequest
 * @author Kevin Beineke
 *         06.09.2012
 */
public class NotifyAboutFailedPeerResponse extends AbstractResponse {

	// Constructors
	/**
	 * Creates an instance of NotifyAboutFailedPeerResponse
	 */
	public NotifyAboutFailedPeerResponse() {
		super();
	}

	/**
	 * Creates an instance of NotifyAboutFailedPeerResponse
	 * @param p_request
	 *            the corresponding NotifyAboutFailedPeerRequest
	 */
	public NotifyAboutFailedPeerResponse(final NotifyAboutFailedPeerRequest p_request) {
		super(p_request, LookupMessages.SUBTYPE_NOTIFY_ABOUT_FAILED_PEER_RESPONSE);
	}

}
