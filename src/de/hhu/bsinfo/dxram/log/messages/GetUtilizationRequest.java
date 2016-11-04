package de.hhu.bsinfo.dxram.log.messages;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Get Utilization Request
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.04.2016
 */
public class GetUtilizationRequest extends AbstractRequest {

    // Constructors

    /**
     * Creates an instance of GetUtilizationRequest
     */
    public GetUtilizationRequest() {
        super();
    }

    /**
     * Creates an instance of GetUtilizationRequest
     *
     * @param p_destination
     *     the destination
     */
    public GetUtilizationRequest(final short p_destination) {
        super(p_destination, DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_GET_UTILIZATION_REQUEST);
    }

}
