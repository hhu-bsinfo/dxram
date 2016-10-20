
package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to the put request.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 17.05.15
 */
public class SuperpeerStoragePutResponse extends AbstractResponse {
	/**
	 * Creates an instance of SuperpeerStoragePutResponse.
	 * This constructor is used when receiving this message.
	 */
	public SuperpeerStoragePutResponse() {
		super();
	}

	/**
	 * Creates an instance of SuperpeerStoragePutResponse.
	 * This constructor is used when sending this message.
	 * @param p_request
	 *            the request
	 */
	public SuperpeerStoragePutResponse(final SuperpeerStoragePutRequest p_request) {
		super(p_request, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_PUT_RESPONSE);
	}
}
