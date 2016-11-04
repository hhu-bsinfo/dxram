package de.hhu.bsinfo.dxram.failure.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Failure Request
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 18.10.2016
 */
public class FailureRequest extends AbstractRequest {

    // Attributes
    private short m_failedNode;

    // Constructors

    /**
     * Creates an instance of FailureRequest
     */
    public FailureRequest() {
        super();

        m_failedNode = -1;
    }

    /**
     * Creates an instance of FailureRequest
     *
     * @param p_destination
     *     the destination
     * @param p_failedNode
     *     the NodeID of the failed node
     */
    public FailureRequest(final short p_destination, final short p_failedNode) {
        super(p_destination, DXRAMMessageTypes.FAILURE_MESSAGES_TYPE, FailureMessages.SUBTYPE_FAILURE_REQUEST);

        m_failedNode = p_failedNode;
    }

    // Getters

    /**
     * Get the failed node
     *
     * @return the NodeID
     */
    public final short getFailedNode() {
        return m_failedNode;
    }

    @Override
    protected final int getPayloadLength() {
        return Short.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putShort(m_failedNode);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_failedNode = p_buffer.getShort();
    }
}
