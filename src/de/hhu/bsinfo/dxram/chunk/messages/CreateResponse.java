package de.hhu.bsinfo.dxram.chunk.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.ChunkMessagesMetadataUtils;
import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Reponse message to the create request.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class CreateResponse extends AbstractResponse {

    private long[] m_chunkIDs;

    /**
     * Creates an instance of CreateResponse.
     * This constructor is used when receiving this message.
     */
    public CreateResponse() {
        super();
    }

    /**
     * Creates an instance of CreateResponse.
     * This constructor is used when sending this message.
     * Make sure to include all the chunks with IDs from the request in the correct order. If a chunk does
     * not exist, no data and a length of 0 indicates this situation.
     *
     * @param p_request
     *     the corresponding GetRequest
     * @param p_chunkIDs
     *     The chunk IDs requested
     */
    public CreateResponse(final CreateRequest p_request, final long... p_chunkIDs) {
        super(p_request, ChunkMessages.SUBTYPE_CREATE_RESPONSE);

        m_chunkIDs = p_chunkIDs;
        setStatusCode(ChunkMessagesMetadataUtils.setNumberOfItemsToSend(getStatusCode(), p_chunkIDs.length));
    }

    /**
     * Get the chunk IDs of the created chunks.
     *
     * @return ChunkIDs.
     */
    public final long[] getChunkIDs() {
        return m_chunkIDs;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        ChunkMessagesMetadataUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_chunkIDs.length);

        for (long chunkID : m_chunkIDs) {
            p_buffer.putLong(chunkID);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        int numChunks = ChunkMessagesMetadataUtils.getNumberOfItemsFromMessageBuffer(getStatusCode(), p_buffer);

        m_chunkIDs = new long[numChunks];

        for (int i = 0; i < m_chunkIDs.length; i++) {
            m_chunkIDs[i] = p_buffer.getLong();
        }
    }

    @Override
    protected final int getPayloadLength() {
        return ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode()) + m_chunkIDs.length * Long.BYTES;
    }
}
