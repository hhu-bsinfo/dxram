package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Get LookupTreeRequest
 *
 * @author Michael Birkhoff, michael.birkhoff@hhu.de, 06.09.2016
 */
public class GetLookupTreeRequest extends AbstractRequest {

    // Attributes
    private short m_nidToGetTreeFrom;

    // Constructors

    /**
     * Creates an instance of GetLookupTreeRequest
     */
    public GetLookupTreeRequest() {
        super();
    }

    /**
     * Creates an instance of GetLookupTreeRequest
     *
     * @param p_destination
     *         the destination
     * @param p_nidToGetTreeFrom
     *         the NodeID
     */
    public GetLookupTreeRequest(final short p_destination, final short p_nidToGetTreeFrom) {

        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_GET_LOOKUP_TREE_REQUEST);

        m_nidToGetTreeFrom = p_nidToGetTreeFrom;
    }

    /**
     * Returns the NodeID
     *
     * @return the NodeID
     */
    public short getTreeNodeID() {
        return m_nidToGetTreeFrom;
    }

    // Methods
    @Override protected final void writePayload(final ByteBuffer p_buffer) {

        p_buffer.putShort(m_nidToGetTreeFrom);

    }

    @Override protected final void readPayload(final ByteBuffer p_buffer) {

        m_nidToGetTreeFrom = p_buffer.getShort();

    }

    @Override protected final int getPayloadLength() {

        return Short.BYTES;
    }

}
