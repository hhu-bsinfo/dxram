package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Init Range Request
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 03.06.2013
 */
public class InitRangeRequest extends AbstractRequest {

    // Attributes
    private long m_startChunkIDOrRangeID;
    private short m_owner;
    private short[] m_backupPeers;
    private boolean m_isBackup;

    // Constructors

    /**
     * Creates an instance of InitRangeRequest
     */
    public InitRangeRequest() {
        super();

        m_startChunkIDOrRangeID = -1;
        m_owner = -1;
        m_backupPeers = null;
        m_isBackup = false;
    }

    /**
     * Creates an instance of InitRangeRequest
     *
     * @param p_destination
     *         the destination
     * @param p_startChunkID
     *         the first object
     * @param p_owner
     *         the owner
     * @param p_backupPeers
     *         the backup peers
     * @param p_isBackup
     *         whether this is a backup message or not
     */
    public InitRangeRequest(final short p_destination, final long p_startChunkID, final short p_owner, final short[] p_backupPeers, final boolean p_isBackup) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_INIT_RANGE_REQUEST);

        m_startChunkIDOrRangeID = p_startChunkID;
        m_owner = p_owner;
        m_backupPeers = p_backupPeers;
        m_isBackup = p_isBackup;
    }

    // Getters

    /**
     * Get the last ChunkID
     *
     * @return the ID
     */
    public final long getStartChunkIDOrRangeID() {
        return m_startChunkIDOrRangeID;
    }

    /**
     * Get owner
     *
     * @return the owner
     */
    public final short getOwner() {
        return m_owner;
    }

    /**
     * Get the backup peers
     *
     * @return the backup peers
     */
    public final short[] getBackupPeers() {
        return m_backupPeers;
    }

    /**
     * Returns whether this is a backup message or not
     *
     * @return whether this is a backup message or not
     */
    public final boolean isBackup() {
        return m_isBackup;
    }

    // Methods
    @Override protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putLong(m_startChunkIDOrRangeID);

        p_buffer.put((byte) m_backupPeers.length);
        if (m_backupPeers.length <= 3) {
            p_buffer.putLong(BackupRange.convert(m_owner, m_backupPeers));
        } else {
            p_buffer.putShort(m_owner);
            for (int i = 0; i < m_backupPeers.length; i++) {
                p_buffer.putShort(m_backupPeers[i]);
            }
        }

        if (m_isBackup) {
            p_buffer.put((byte) 1);
        } else {
            p_buffer.put((byte) 0);
        }
    }

    @Override protected final void readPayload(final ByteBuffer p_buffer) {
        byte size;

        m_startChunkIDOrRangeID = p_buffer.getLong();

        size = p_buffer.get();
        if (size <= 3) {
            long tmp = p_buffer.getLong();
            m_owner = (short) tmp;
            m_backupPeers = BackupRange.convert(tmp);
        } else {
            m_owner = p_buffer.getShort();
            m_backupPeers = new short[size];
            for (int i = 0; i < size; i++) {
                m_backupPeers[i] = p_buffer.getShort();
            }
        }

        final byte b = p_buffer.get();
        if (b == 1) {
            m_isBackup = true;
        }
    }

    @Override protected final int getPayloadLength() {
        int ret;

        if (m_backupPeers.length <= 3) {
            ret = 2 * Long.BYTES + 2 * Byte.BYTES;
        } else {
            ret = Long.BYTES + (1 + m_backupPeers.length) * Short.BYTES + 2 * Byte.BYTES;
        }

        return ret;
    }

}
