package de.uniduesseldorf.dxram.core.lookup.messages;

import de.uniduesseldorf.menet.AbstractRequest;

/**
 * Search for Peer Request
 * @author Kevin Beineke
 *         06.09.2012
 */
public class SearchForPeerRequest extends AbstractRequest {

	// Constructors
	/**
	 * Creates an instance of SearchForPeerRequest
	 */
	public SearchForPeerRequest() {
		super();
	}

	/**
	 * Creates an instance of SearchForPeerRequest
	 * @param p_destination
	 *            the destination
	 */
	public SearchForPeerRequest(final short p_destination) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_SEARCH_FOR_PEER_REQUEST);
	}

}
