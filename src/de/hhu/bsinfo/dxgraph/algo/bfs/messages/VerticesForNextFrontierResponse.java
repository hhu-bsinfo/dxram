
package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

import de.hhu.bsinfo.menet.AbstractRequest;
import de.hhu.bsinfo.menet.AbstractResponse;

/**
 * Response to the request.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 13.05.16
 */
public class VerticesForNextFrontierResponse extends AbstractResponse {
	/**
	 * Creates an instance of VerticesForNextFrontierRequest.
	 * This constructor is used when receiving this message.
	 */
	public VerticesForNextFrontierResponse() {
		super();
	}

	/**
	 * Creates an instance of VerticesForNextFrontierRequest
	 * @param p_request
	 *            Request to respond to
	 */
	public VerticesForNextFrontierResponse(final AbstractRequest p_request) {
		super(p_request, BFSMessages.SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_RESPONSE);
	}
}
