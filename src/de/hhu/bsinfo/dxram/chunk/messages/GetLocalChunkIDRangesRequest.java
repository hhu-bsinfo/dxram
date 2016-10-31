
package de.hhu.bsinfo.dxram.chunk.messages;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Request for getting the chunk id ranges of locally stored chunk ids from another node.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class GetLocalChunkIDRangesRequest extends AbstractRequest {
    /**
     * Creates an instance of GetLocalChunkIDRangesRequest.
     * This constructor is used when receiving this message.
     */
    public GetLocalChunkIDRangesRequest() {
        super();
    }

    /**
     * Creates an instance of GetLocalChunkIDRangesRequest.
     * This constructor is used when sending this message.
     * @param p_destination
     *            the destination node id.
     */
    public GetLocalChunkIDRangesRequest(final short p_destination) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_LOCAL_CHUNKID_RANGES_REQUEST);
    }
}
