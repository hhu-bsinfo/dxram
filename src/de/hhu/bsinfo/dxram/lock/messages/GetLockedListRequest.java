
package de.hhu.bsinfo.dxram.lock.messages;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Request to get a list of locked chunks from another node.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 19.04.2016
 */
public class GetLockedListRequest extends AbstractRequest {

    /**
     * Creates an instance of GetLockedListRequest as a receiver.
     */
    public GetLockedListRequest() {
        super();
    }

    /**
     * Creates an instance of GetLockedListRequest as a sender
     * @param p_destination
     *            the destination node ID.
     */
    public GetLockedListRequest(final short p_destination) {
        super(p_destination, DXRAMMessageTypes.LOCK_MESSAGES_TYPE, LockMessages.SUBTYPE_GET_LOCKED_LIST_REQUEST);
    }

}
