package de.hhu.bsinfo.dxram.net.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.ethnet.AbstractMessage;

/**
 * This is a default message which is never processed on the receiver.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.10.2016
 */
public class DefaultMessage extends AbstractMessage {

    /**
     * Creates an instance of DefaultMessage.
     */
    public DefaultMessage() {
        super();
    }

    /**
     * Creates an instance of DefaultMessage
     *
     * @param p_destination
     *         the destination nodeID
     */
    public DefaultMessage(final short p_destination) {
        super(p_destination, DXRAMMessageTypes.DEFAULT_MESSAGES_TYPE, DefaultMessages.SUBTYPE_DEFAULT_MESSAGE);
    }

    // Methods
    @Override protected final void writePayload(final ByteBuffer p_buffer) {
    }

    @Override protected final void readPayload(final ByteBuffer p_buffer) {
    }

    @Override protected final int getPayloadLength() {
        return 0;
    }

}
