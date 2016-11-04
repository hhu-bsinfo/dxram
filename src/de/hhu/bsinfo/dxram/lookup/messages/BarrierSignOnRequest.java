package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Request to sign on at a barrier
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.05.2016
 */
public class BarrierSignOnRequest extends AbstractRequest {
    private int m_barrierId = -1;
    private long m_customData = -1;

    /**
     * Creates an instance of SlaveSyncBarrierSignOnMessage.
     * This constructor is used when receiving this message.
     */
    public BarrierSignOnRequest() {
        super();
    }

    /**
     * Creates an instance of SlaveSyncBarrierSignOnMessage.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *     the destination node id.
     * @param p_barrierId
     *     Id of the barrier to sign on
     * @param p_customData
     *     Custom data to pass along with the sign on
     */
    public BarrierSignOnRequest(final short p_destination, final int p_barrierId, final long p_customData) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_BARRIER_SIGN_ON_REQUEST);

        m_barrierId = p_barrierId;
        m_customData = p_customData;
    }

    /**
     * Get the id of the barrier to sign on.
     *
     * @return Barrier id
     */
    public int getBarrierId() {
        return m_barrierId;
    }

    /**
     * Get the custom data to be passed along with the sign on.
     *
     * @return Custom data for sign on
     */
    public long getCustomData() {
        return m_customData;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + Long.BYTES;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putInt(m_barrierId);
        p_buffer.putLong(m_customData);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_barrierId = p_buffer.getInt();
        m_customData = p_buffer.getLong();
    }
}
