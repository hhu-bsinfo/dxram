package de.hhu.bsinfo.dxram.chunk.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.ChunkMessagesMetadataUtils;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Request to create new chunks remotely.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 13.01.2016
 */
public class CreateRequest extends AbstractRequest {
    private int[] m_sizes;

    /**
     * Creates an instance of CreateRequest.
     * This constructor is used when receiving this message.
     */
    public CreateRequest() {
        super();
    }

    /**
     * Creates an instance of CreateRequest.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *     the destination node id.
     * @param p_sizes
     *     Sizes of the chunks to create.
     */
    public CreateRequest(final short p_destination, final int... p_sizes) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_CREATE_REQUEST);

        m_sizes = p_sizes;

        setStatusCode(ChunkMessagesMetadataUtils.setNumberOfItemsToSend(getStatusCode(), p_sizes.length));
    }

    /**
     * Get the sizes received.
     *
     * @return Array of sizes to create chunks of.
     */
    public int[] getSizes() {
        return m_sizes;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        ChunkMessagesMetadataUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_sizes.length);

        for (int size : m_sizes) {
            p_buffer.putInt(size);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        int numSizes = ChunkMessagesMetadataUtils.getNumberOfItemsFromMessageBuffer(getStatusCode(), p_buffer);

        m_sizes = new int[numSizes];
        for (int i = 0; i < m_sizes.length; i++) {
            m_sizes[i] = p_buffer.getInt();
        }
    }

    @Override
    protected final int getPayloadLength() {
        return ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode()) + Integer.BYTES * m_sizes.length;
    }
}
