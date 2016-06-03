
package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.menet.AbstractRequest;

/**
 * Request for getting the number of mappings
 * @author klein 26.03.2015
 */
public class GetNameserviceEntryCountRequest extends AbstractRequest {

	// Constructors
	/**
	 * Creates an instance of GetMappingCountRequest
	 */
	public GetNameserviceEntryCountRequest() {
		super();
	}

	/**
	 * Creates an instance of GetMappingCountRequest
	 * @param p_destination
	 *            the destination
	 */
	public GetNameserviceEntryCountRequest(final short p_destination) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRY_COUNT_REQUEST);
	}

}
