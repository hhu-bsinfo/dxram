package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Message to notify peers about an update in the nameservice to update their local caches
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 19.04.2016
 */
public class NameserviceUpdatePeerCachesMessage extends AbstractRequest {

    // Attributes
    private int m_id;
    private long m_chunkID;

    // Constructors

    /**
     * Creates an instance of NameserviceUpdatePeerCachesMessage
     */
    public NameserviceUpdatePeerCachesMessage() {
        super();

        m_id = -1;
        m_chunkID = ChunkID.INVALID_ID;
    }

    /**
     * Creates an instance of NameserviceUpdatePeerCachesMessage
     *
     * @param p_destination
     *     the destination
     * @param p_id
     *     the id to store
     * @param p_chunkID
     *     the ChunkID to store
     */
    public NameserviceUpdatePeerCachesMessage(final short p_destination, final int p_id, final long p_chunkID) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_NAMESERVICE_UPDATE_PEER_CACHES_MESSAGE);

        m_id = p_id;
        m_chunkID = p_chunkID;
    }

    // Getters

    /**
     * Get the id to store
     *
     * @return the id to store
     */
    public final int getID() {
        return m_id;
    }

    /**
     * Get the ChunkID to store
     *
     * @return the ChunkID to store
     */
    public final long getChunkID() {
        return m_chunkID;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + Long.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putInt(m_id);
        p_buffer.putLong(m_chunkID);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_id = p_buffer.getInt();
        m_chunkID = p_buffer.getLong();
    }

}
