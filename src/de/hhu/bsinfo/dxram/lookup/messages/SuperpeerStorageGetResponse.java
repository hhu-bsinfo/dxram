package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to the get request.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.2015
 */
public class SuperpeerStorageGetResponse extends AbstractResponse {
    // The chunk objects here are used when sending the response only
    // when the response is received, the data structures from the request are
    // used to directly write the data to them and avoiding further copying
    private DataStructure m_dataStructure;

    /**
     * Creates an instance of SuperpeerStorageGetResponse.
     * This constructor is used when receiving this message.
     */
    public SuperpeerStorageGetResponse() {
        super();
    }

    /**
     * Creates an instance of GetResponse.
     * This constructor is used when sending this message.
     * Make sure to include all the chunks with IDs from the request in the correct order. If a chunk does
     * not exist, no data and a length of 0 indicates this situation.
     *
     * @param p_request
     *         the corresponding GetRequest
     * @param p_dataStructure
     *         Data structure filled with the read data from memory
     */
    public SuperpeerStorageGetResponse(final SuperpeerStorageGetRequest p_request, final DataStructure p_dataStructure) {
        super(p_request, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_GET_RESPONSE);

        m_dataStructure = p_dataStructure;
    }

    @Override protected final void writePayload(final ByteBuffer p_buffer) {
        // read the data to be sent to the remote from the chunk set for this message
        MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);
        int size = m_dataStructure.sizeofObject();
        exporter.setPayloadSize(size);
        p_buffer.putInt(size);
        p_buffer.order(ByteOrder.nativeOrder());
        exporter.exportObject(m_dataStructure);
        p_buffer.order(ByteOrder.BIG_ENDIAN);
    }

    @Override protected final void readPayload(final ByteBuffer p_buffer) {
        // read the payload from the buffer and write it directly into
        // the data structure provided by the request to avoid further copying of data
        MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);
        SuperpeerStorageGetRequest request = (SuperpeerStorageGetRequest) getCorrespondingRequest();

        m_dataStructure = request.getDataStructure();

        importer.setPayloadSize(p_buffer.getInt());
        p_buffer.order(ByteOrder.nativeOrder());
        importer.importObject(m_dataStructure);
        p_buffer.order(ByteOrder.BIG_ENDIAN);
    }

    @Override protected final int getPayloadLength() {
        return Integer.BYTES + m_dataStructure.sizeofObject();
    }
}
