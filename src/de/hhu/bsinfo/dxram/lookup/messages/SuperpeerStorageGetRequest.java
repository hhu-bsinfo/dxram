package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Request to get data from the superpeer storage.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.2015
 */
public class SuperpeerStorageGetRequest extends AbstractRequest {
    // the data structure is stored for the sender of the request
    // to write the incoming data of the response to it
    // the requesting IDs are taken from the structures
    private DataStructure m_dataStructure;
    // this is only used when receiving the request
    private int m_storageID;

    /**
     * Creates an instance of SuperpeerStorageGetRequest.
     * This constructor is used when receiving this message.
     */
    public SuperpeerStorageGetRequest() {
        super();
    }

    /**
     * Creates an instance of SuperpeerStorageGetRequest.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *         the destination node id.
     * @param p_dataStructure
     *         Data structure with the ID of the chunk to get.
     */
    public SuperpeerStorageGetRequest(final short p_destination, final DataStructure p_dataStructure) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_GET_REQUEST);

        m_dataStructure = p_dataStructure;
    }

    /**
     * Get the storage id.
     *
     * @return Storage id.
     */
    public int getStorageID() {
        return m_storageID;
    }

    /**
     * Get the data structures stored with this request.
     * This is used to write the received data to the provided object to avoid
     * using multiple buffers.
     *
     * @return Data structures to store data to when the response arrived.
     */
    public DataStructure getDataStructure() {
        return m_dataStructure;
    }

    @Override protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putInt((int) m_dataStructure.getID());
    }

    @Override protected final void readPayload(final ByteBuffer p_buffer) {
        m_storageID = p_buffer.getInt();
    }

    @Override protected final int getPayloadLength() {
        return Integer.BYTES;
    }
}
