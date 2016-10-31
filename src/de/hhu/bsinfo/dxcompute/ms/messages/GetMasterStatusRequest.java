
package de.hhu.bsinfo.dxcompute.ms.messages;

import de.hhu.bsinfo.dxcompute.DXComputeMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Get the current status of a master compute node.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class GetMasterStatusRequest extends AbstractRequest {
    /**
     * Creates an instance of GetMasterStatusRequest.
     * This constructor is used when receiving this message.
     */
    public GetMasterStatusRequest() {
        super();
    }

    /**
     * Creates an instance of GetMasterStatusRequest.
     * This constructor is used when sending this message.
     * @param p_destination
     *            the destination node id.
     */
    public GetMasterStatusRequest(final short p_destination) {
        super(p_destination, DXComputeMessageTypes.MASTERSLAVE_MESSAGES_TYPE, MasterSlaveMessages.SUBTYPE_GET_MASTER_STATUS_REQUEST);
    }
}
