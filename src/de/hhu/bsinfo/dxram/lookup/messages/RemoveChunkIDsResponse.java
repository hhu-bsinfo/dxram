package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to a RemoveRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class RemoveChunkIDsResponse extends AbstractResponse {

    // Attributes
    private short[] m_backupSuperpeers;

    // Constructors

    /**
     * Creates an instance of RemoveResponse
     */
    public RemoveChunkIDsResponse() {
        super();

        m_backupSuperpeers = null;
    }

    /**
     * Creates an instance of RemoveResponse
     *
     * @param p_request
     *     the corresponding RemoveRequest
     * @param p_backupSuperpeers
     *     the backup superpeers
     */
    public RemoveChunkIDsResponse(final RemoveChunkIDsRequest p_request, final short[] p_backupSuperpeers) {
        super(p_request, LookupMessages.SUBTYPE_REMOVE_CHUNKIDS_RESPONSE);

        m_backupSuperpeers = p_backupSuperpeers;
    }

    // Getters

    /**
     * Get the backup superpeers
     *
     * @return the backup superpeers
     */
    public final short[] getBackupSuperpeers() {
        return m_backupSuperpeers;
    }

    @Override
    protected final int getPayloadLength() {
        int ret;

        if (m_backupSuperpeers == null) {
            ret = Byte.BYTES;
        } else {
            ret = Byte.BYTES + Integer.BYTES + Short.BYTES * m_backupSuperpeers.length;
        }

        return ret;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        if (m_backupSuperpeers == null) {
            p_buffer.put((byte) 0);
        } else {
            p_buffer.put((byte) 1);
            p_buffer.putInt(m_backupSuperpeers.length);
            p_buffer.asShortBuffer().put(m_backupSuperpeers);
            p_buffer.position(p_buffer.position() + Short.BYTES * m_backupSuperpeers.length);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        if (p_buffer.get() != 0) {
            m_backupSuperpeers = new short[p_buffer.getInt()];
            p_buffer.asShortBuffer().get(m_backupSuperpeers);
            p_buffer.position(p_buffer.position() + Short.BYTES * m_backupSuperpeers.length);
        }
    }

}
