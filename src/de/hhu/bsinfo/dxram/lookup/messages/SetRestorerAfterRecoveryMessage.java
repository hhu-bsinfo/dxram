package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractMessage;

/**
 * Update All Message
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.10.2015
 */
public class SetRestorerAfterRecoveryMessage extends AbstractMessage {

    // Attributes
    private short m_owner;

    // Constructors

    /**
     * Creates an instance of UpdateAllMessage
     */
    public SetRestorerAfterRecoveryMessage() {
        super();

        m_owner = -1;
    }

    /**
     * Creates an instance of UpdateAllMessage
     *
     * @param p_destination
     *     the destination
     * @param p_owner
     *     the failed peer
     */
    public SetRestorerAfterRecoveryMessage(final short p_destination, final short p_owner) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_SET_RESTORER_AFTER_RECOVERY_MESSAGE);

        m_owner = p_owner;
    }

    // Getters

    /**
     * Get the owner
     *
     * @return the NodeID
     */
    public final short getOwner() {
        return m_owner;
    }

    @Override
    protected final int getPayloadLength() {
        return Short.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putShort(m_owner);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_owner = p_buffer.getShort();
    }

}
