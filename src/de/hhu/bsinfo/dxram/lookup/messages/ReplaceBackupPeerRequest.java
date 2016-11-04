package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Replace Backup Peer Request
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 21.10.2016
 */
public class ReplaceBackupPeerRequest extends AbstractRequest {

    // Attributes
    private long m_firstChunkIDOrRangeID;
    private short m_failedBackupPeer;
    private short m_newBackupPeer;
    private boolean m_isBackup;

    // Constructors

    /**
     * Creates an instance of ReplaceBackupPeerRequest
     */
    public ReplaceBackupPeerRequest() {
        super();

        m_firstChunkIDOrRangeID = -1;
        m_failedBackupPeer = -1;
        m_newBackupPeer = -1;
        m_isBackup = false;
    }

    /**
     * Creates an instance of ReplaceBackupPeerRequest
     *
     * @param p_destination
     *     the destination
     * @param p_firstChunkIDOrRangeID
     *     the first ChunkID or RangeID
     * @param p_failedBackupPeer
     *     the failed backup peer
     * @param p_newBackupPeer
     *     the replacement
     * @param p_isBackup
     *     whether this is a backup or not
     */
    public ReplaceBackupPeerRequest(final short p_destination, final long p_firstChunkIDOrRangeID, final short p_failedBackupPeer, final short p_newBackupPeer,
        final boolean p_isBackup) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_REPLACE_BACKUP_PEER_REQUEST);

        m_firstChunkIDOrRangeID = p_firstChunkIDOrRangeID;
        m_failedBackupPeer = p_failedBackupPeer;
        m_newBackupPeer = p_newBackupPeer;
        m_isBackup = p_isBackup;
    }

    // Getters

    /**
     * Get the first ChunkID
     *
     * @return the ID
     */
    public final long getFirstChunkIDOrRangeIDhunkID() {
        return m_firstChunkIDOrRangeID;
    }

    /**
     * Get the NodeID of the failed peer
     *
     * @return the NodeID
     */
    public final short getFailedPeer() {
        return m_failedBackupPeer;
    }

    /**
     * Get the NodeID of the new backup peer
     *
     * @return the NodeID
     */
    public final short getNewPeer() {
        return m_newBackupPeer;
    }

    /**
     * Return if is is a backup
     *
     * @return whether this is a backup or not
     */
    public final boolean isBackup() {
        return m_isBackup;
    }

    @Override
    protected final int getPayloadLength() {
        return Long.BYTES + Short.BYTES * 2 + Byte.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putLong(m_firstChunkIDOrRangeID);
        p_buffer.putShort(m_failedBackupPeer);
        p_buffer.putShort(m_newBackupPeer);
        p_buffer.put((byte) (m_isBackup ? 1 : 0));
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_firstChunkIDOrRangeID = p_buffer.getLong();
        m_failedBackupPeer = p_buffer.getShort();
        m_newBackupPeer = p_buffer.getShort();
        m_isBackup = p_buffer.get() == 1;
    }

}
