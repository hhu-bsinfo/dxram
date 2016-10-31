package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Change the size of an existing barrier.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 06.05.2016
 */
public class BarrierChangeSizeRequest extends AbstractRequest {
    private int m_barrierId;
    private int m_size;
    private boolean m_isReplicate;

    /**
     * Creates an instance of BarrierChangeSizeRequest
     */
    public BarrierChangeSizeRequest() {
        super();
    }

    /**
     * Creates an instance of BarrierChangeSizeRequest
     *
     * @param p_destination
     *         the destination
     * @param p_barrierId
     *         size of the barrier
     * @param p_size
     *         id of the barrier
     * @param p_isReplicate
     *         wether it is a replicate or not
     */
    public BarrierChangeSizeRequest(final short p_destination, final int p_barrierId, final int p_size, final boolean p_isReplicate) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_BARRIER_CHANGE_SIZE_REQUEST);

        m_barrierId = p_barrierId;
        m_size = p_size;
        m_isReplicate = p_isReplicate;
    }

    /**
     * Id of the barrier to change the size of.
     *
     * @return Barrier id
     */
    public int getBarrierId() {
        return m_barrierId;
    }

    /**
     * Get the barrier size;
     *
     * @return Barrier size
     */
    public int getBarrierSize() {
        return m_size;
    }

    /**
     * Returns if it is a replicate or not.
     *
     * @return True if it is a replicate, false otherwise.
     */
    public boolean isReplicate() {
        return m_isReplicate;
    }

    @Override protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putInt(m_barrierId);
        p_buffer.putInt(m_size);
        p_buffer.put(m_isReplicate ? (byte) 1 : (byte) 0);
    }

    @Override protected final void readPayload(final ByteBuffer p_buffer) {
        m_barrierId = p_buffer.getInt();
        m_size = p_buffer.getInt();
        m_isReplicate = p_buffer.get() == (byte) 1;
    }

    @Override protected final int getPayloadLength() {
        return Integer.BYTES + Integer.BYTES + Byte.BYTES;
    }
}
