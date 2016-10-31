
package de.hhu.bsinfo.dxram.chunk.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.ChunkMessagesMetadataUtils;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Request for getting a Chunk from a remote node
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.12.2015
 */
public class GetRequest extends AbstractRequest {

    // the data structure is stored for the sender of the request
    // to write the incoming data of the response to it
    // the requesting IDs are taken from the structures
    private DataStructure[] m_dataStructures;
    // this is only used when receiving the request
    private long[] m_chunkIDs;

    /**
     * Creates an instance of GetRequest.
     * This constructor is used when receiving this message.
     */
    public GetRequest() {
        super();
    }

    /**
     * Creates an instance of GetRequest.
     * This constructor is used when sending this message.
     * @param p_destination
     *            the destination node id.
     * @param p_dataStructures
     *            Data structure with the ID of the chunk to get.
     */
    public GetRequest(final short p_destination, final DataStructure... p_dataStructures) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_REQUEST);

        m_dataStructures = p_dataStructures;

        byte tmpCode = getStatusCode();
        setStatusCode(ChunkMessagesMetadataUtils.setNumberOfItemsToSend(tmpCode, p_dataStructures.length));
    }

    /**
     * Get the chunk IDs of this request (when receiving it).
     * @return Chunk ID.
     */
    public long[] getChunkIDs() {
        return m_chunkIDs;
    }

    /**
     * Get the data structures stored with this request.
     * This is used to write the received data to the provided object to avoid
     * using multiple buffers.
     * @return Data structures to store data to when the response arrived.
     */
    public DataStructure[] getDataStructures() {
        return m_dataStructures;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        ChunkMessagesMetadataUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_dataStructures.length);

        for (DataStructure dataStructure : m_dataStructures) {
            p_buffer.putLong(dataStructure.getID());
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
        if (m_dataStructures != null) {
            return ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode()) + Long.BYTES * m_dataStructures.length;
        } else {
            return ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode()) + Long.BYTES * m_chunkIDs.length;
        }
    }
}
