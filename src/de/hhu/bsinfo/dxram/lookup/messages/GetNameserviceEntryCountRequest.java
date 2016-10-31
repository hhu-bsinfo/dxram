package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Request for getting the number of mappings
 *
 * @author Florian Klein, florian.klein@hhu.de, 26.03.2015
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
     *
     * @param p_destination
     *         the destination
     */
    public GetNameserviceEntryCountRequest(final short p_destination) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRY_COUNT_REQUEST);
    }

}
