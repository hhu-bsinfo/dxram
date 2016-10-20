
package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.menet.AbstractRequest;

/**
 * Requesting the status of the superpeer storage.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.05.15
 */
public class SuperpeerStorageStatusRequest extends AbstractRequest {
	/**
	 * Creates an instance of SuperpeerStorageCreateRequest
	 */
	public SuperpeerStorageStatusRequest() {
		super();
	}

	/**
	 * Creates an instance of SuperpeerStorageCreateRequest
	 * @param p_destination
	 *            the destination
	 */
	public SuperpeerStorageStatusRequest(final short p_destination) {
		super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
				LookupMessages.SUBTYPE_SUPERPEER_STORAGE_STATUS_REQUEST);
	}
}
