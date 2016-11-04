package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Request the status of a barrier
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.05.2016
 */
public class BarrierGetStatusRequest extends AbstractRequest {
    private int m_barrierId = -1;

    /**
     * Creates an instance of BarrierGetStatusRequest.
     * This constructor is used when receiving this message.
     */
    public BarrierGetStatusRequest() {
        super();
    }

    /**
     * Creates an instance of BarrierGetStatusRequest.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *     the destination node id.
     * @param p_barrierId
     *     Id of the barrier to get the status of
     */
    public BarrierGetStatusRequest(final short p_destination, final int p_barrierId) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_BARRIER_STATUS_REQUEST);

        m_barrierId = p_barrierId;
    }

    /**
     * Get the id of the barrier to get the status of
     *
     * @return Barrier id
     */
    public int getBarrierId() {
        return m_barrierId;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putInt(m_barrierId);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_barrierId = p_buffer.getInt();
    }
}
