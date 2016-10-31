package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Join Request
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class JoinRequest extends AbstractRequest {

    // Attributes
    private short m_newNode;
    private boolean m_nodeIsSuperpeer;

    // Constructors

    /**
     * Creates an instance of JoinRequest
     */
    public JoinRequest() {
        super();

        m_newNode = -1;
        m_nodeIsSuperpeer = false;
    }

    /**
     * Creates an instance of JoinRequest
     *
     * @param p_destination
     *         the destination
     * @param p_newNode
     *         the NodeID of the new node
     * @param p_nodeIsSuperpeer
     *         wether the new node is a superpeer or not
     */
    public JoinRequest(final short p_destination, final short p_newNode, final boolean p_nodeIsSuperpeer) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_JOIN_REQUEST);

        assert p_newNode != 0;

        m_newNode = p_newNode;
        m_nodeIsSuperpeer = p_nodeIsSuperpeer;
    }

    // Getters

    /**
     * Get new node
     *
     * @return the NodeID
     */
    public final short getNewNode() {
        return m_newNode;
    }

    /**
     * Get role of new node
     *
     * @return true if the new node is a superpeer, false otherwise
     */
    public final boolean nodeIsSuperpeer() {
        return m_nodeIsSuperpeer;
    }

    // Methods
    @Override protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putShort(m_newNode);

        if (m_nodeIsSuperpeer) {
            p_buffer.put((byte) 1);
        } else {
            p_buffer.put((byte) 0);
        }
    }

    @Override protected final void readPayload(final ByteBuffer p_buffer) {
        m_newNode = p_buffer.getShort();

        final byte b = p_buffer.get();
        if (b == 1) {
            m_nodeIsSuperpeer = true;
        }
    }

    @Override protected final int getPayloadLength() {
        return Short.BYTES + Byte.BYTES;
    }

}
