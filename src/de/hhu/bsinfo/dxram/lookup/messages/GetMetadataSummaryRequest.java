package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Request for getting the a metadata summary
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.10.2016
 */
public class GetMetadataSummaryRequest extends AbstractRequest {

    // Constructors

    /**
     * Created because compiler
     */
    public GetMetadataSummaryRequest() {
        super();
    }

    /**
     * Creates an instance of LogMessage
     *
     * @param p_destination
     *     the destination
     */
    public GetMetadataSummaryRequest(final short p_destination) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_GET_METADATA_SUMMARY_REQUEST);
    }

    @Override
    protected final int getPayloadLength() {
        return 0;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
    }

}
