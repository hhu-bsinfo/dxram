package de.hhu.bsinfo.dxram.chunk.messages;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Request to get status information from a remote chunk service.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 07.04.2016
 */
public class StatusRequest extends AbstractRequest {
    /**
     * Creates an instance of StatusRequest.
     * This constructor is used when receiving this message.
     */
    public StatusRequest() {
        super();
    }

    /**
     * Creates an instance of StatusRequest.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *     the destination node id.
     */
    public StatusRequest(final short p_destination) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_STATUS_REQUEST);
    }
}
