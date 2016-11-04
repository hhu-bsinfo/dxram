package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to the barrier alloc request
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.05.2016
 */
public class BarrierAllocResponse extends AbstractResponse {
    private int m_barrierId;

    /**
     * Creates an instance of BarrierAllocResponse
     */
    public BarrierAllocResponse() {
        super();

    }

    /**
     * Creates an instance of BarrierAllocResponse
     *
     * @param p_request
     *     the corresponding BarrierAllocRequest
     * @param p_barrierId
     *     Id of the created barrier
     */
    public BarrierAllocResponse(final BarrierAllocRequest p_request, final int p_barrierId) {
        super(p_request, LookupMessages.SUBTYPE_BARRIER_ALLOC_RESPONSE);

        m_barrierId = p_barrierId;
    }

    /**
     * Get the id of the created barrier.
     *
     * @return Barrier id.
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
