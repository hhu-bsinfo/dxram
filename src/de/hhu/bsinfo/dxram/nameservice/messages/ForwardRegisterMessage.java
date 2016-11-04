package de.hhu.bsinfo.dxram.nameservice.messages;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractMessage;

/**
 * (Async) Message for updating a Chunk on a remote node
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.12.2015
 */
public class ForwardRegisterMessage extends AbstractMessage {

    private long m_chunkId;
    private String m_name;

    /**
     * Creates an instance of RegisterMessage.
     * This constructor is used when receiving this message.
     */
    public ForwardRegisterMessage() {
        super();
    }

    /**
     * Creates an instance of RegisterMessage
     *
     * @param p_destination
     *     the destination
     * @param p_chunkId
     *     The chunk id to register a mapping for.
     * @param p_name
     *     The name to use for the mapping of the chunk id
     */
    public ForwardRegisterMessage(final short p_destination, final long p_chunkId, final String p_name) {
        super(p_destination, DXRAMMessageTypes.NAMESERVICE_MESSAGES_TYPE, NameserviceMessages.SUBTYPE_REGISTER_MESSAGE);

        m_chunkId = p_chunkId;
        m_name = p_name;
    }

    /**
     * Get the chunk id for registration.
     *
     * @return Chunk id
     */
    public long getChunkId() {
        return m_chunkId;
    }

    /**
     * Get the name to use for registration of the chunk id.
     *
     * @return Name
     */
    public String getName() {
        return m_name;
    }

    @Override
    protected final int getPayloadLength() {
        return Long.BYTES + Integer.BYTES + m_name.length();
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putLong(m_chunkId);
        p_buffer.putInt(m_name.length());
        p_buffer.put(m_name.getBytes(StandardCharsets.US_ASCII));
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_chunkId = p_buffer.getLong();
        int sizeStr = p_buffer.getInt();
        byte[] tmp = new byte[sizeStr];
        p_buffer.get(tmp);
        m_name = new String(tmp, StandardCharsets.US_ASCII);
    }

}
