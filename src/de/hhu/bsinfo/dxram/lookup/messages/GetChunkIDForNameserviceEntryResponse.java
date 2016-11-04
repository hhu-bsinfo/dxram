package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to a GetChunkIDRequest
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 */
public class GetChunkIDForNameserviceEntryResponse extends AbstractResponse {

    // Attributes
    private long m_chunkID;

    // Constructors

    /**
     * Creates an instance of GetChunkIDResponse
     */
    public GetChunkIDForNameserviceEntryResponse() {
        super();

        m_chunkID = ChunkID.INVALID_ID;
    }

    /**
     * Creates an instance of GetChunkIDResponse
     *
     * @param p_request
     *     the request
     * @param p_chunkID
     *     the ChunkID
     */
    public GetChunkIDForNameserviceEntryResponse(final GetChunkIDForNameserviceEntryRequest p_request, final long p_chunkID) {
        super(p_request, LookupMessages.SUBTYPE_GET_CHUNKID_FOR_NAMESERVICE_ENTRY_RESPONSE);

        m_chunkID = p_chunkID;
    }

    // Getters

    /**
     * Get the ChunkID
     *
     * @return the ChunkID
     */
    public final long getChunkID() {
        return m_chunkID;
    }

    @Override
    protected final int getPayloadLength() {
        return Long.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putLong(m_chunkID);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_chunkID = p_buffer.getLong();
    }

}
