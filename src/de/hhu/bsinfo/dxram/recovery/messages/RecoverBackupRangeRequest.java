package de.hhu.bsinfo.dxram.recovery.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Recover Backup Range Request
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 08.10.2015
 */
public class RecoverBackupRangeRequest extends AbstractRequest {

    // Attributes
    private short m_owner;
    private long m_firstChunkIDOrRangeID;

    // Constructors

    /**
     * Creates an instance of RecoverBackupRangeRequest
     */
    public RecoverBackupRangeRequest() {
        super();

        m_owner = (short) -1;
        m_firstChunkIDOrRangeID = -1;
    }

    /**
     * Creates an instance of RecoverBackupRangeRequest
     *
     * @param p_destination
     *     the destination
     * @param p_owner
     *     the NodeID of the owner
     * @param p_firstChunkIDOrRangeID
     *     the first ChunkID of the backup range or the RangeID for migrations
     */
    public RecoverBackupRangeRequest(final short p_destination, final short p_owner, final long p_firstChunkIDOrRangeID) {
        super(p_destination, DXRAMMessageTypes.RECOVERY_MESSAGES_TYPE, RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_REQUEST);

        m_owner = p_owner;
        m_firstChunkIDOrRangeID = p_firstChunkIDOrRangeID;
    }

    // Getters

    /**
     * Get the owner
     *
     * @return the NodeID
     */
    public final short getOwner() {
        return m_owner;
    }

    /**
     * Get the ChunkID or RangeID
     *
     * @return the ChunkID or RangeID
     */
    public final long getFirstChunkIDOrRangeID() {
        return m_firstChunkIDOrRangeID;
    }

    @Override
    protected final int getPayloadLength() {
        return Short.BYTES + Long.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putShort(m_owner);
        p_buffer.putLong(m_firstChunkIDOrRangeID);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_owner = p_buffer.getShort();
        m_firstChunkIDOrRangeID = p_buffer.getLong();
    }

}
