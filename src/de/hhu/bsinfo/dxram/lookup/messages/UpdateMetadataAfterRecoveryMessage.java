/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.backup.RangeID;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Update All Message
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.10.2015
 */
public class UpdateMetadataAfterRecoveryMessage extends AbstractMessage {

    // Attributes
    private short m_rangeID;
    private short m_creator;
    private short m_restorer;
    private long[] m_chunkIDRanges;

    // Constructors

    /**
     * Creates an instance of UpdateMetadataAfterRecoveryMessage
     */
    public UpdateMetadataAfterRecoveryMessage() {
        super();

        m_rangeID = RangeID.INVALID_ID;
        m_creator = NodeID.INVALID_ID;
        m_restorer = NodeID.INVALID_ID;
        m_chunkIDRanges = null;
    }

    /**
     * Creates an instance of UpdateMetadataAfterRecoveryMessage
     *
     * @param p_destination
     *     the destination
     * @param p_rangeID
     *     the RangeID
     * @param p_creator
     *     the creator of all chunks
     * @param p_restorer
     *     the recovery peer
     * @param p_chunkIDRanges
     *     all recovered ChunkIDs in ranges
     */
    public UpdateMetadataAfterRecoveryMessage(final short p_destination, final short p_rangeID, final short p_creator, final short p_restorer,
        final long[] p_chunkIDRanges) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_UPDATE_METADATA_AFTER_RECOVERY_MESSAGE);

        m_rangeID = p_rangeID;
        m_creator = p_creator;
        m_restorer = p_restorer;
        m_chunkIDRanges = p_chunkIDRanges;
    }

    // Getters

    /**
     * Get the RangeID
     *
     * @return the RangeID
     */
    public final short getRangeID() {
        return m_rangeID;
    }

    /**
     * Get the creator
     *
     * @return the creator
     */
    public final short getCreator() {
        return m_creator;
    }

    /**
     * Get the restorer
     *
     * @return the restorer
     */
    public final short getRestorer() {
        return m_restorer;
    }

    /**
     * Get the creator
     *
     * @return the creator
     */
    public final long[] getChunkIDRanges() {
        return m_chunkIDRanges;
    }

    @Override
    protected final int getPayloadLength() {
        return 3 * Short.BYTES + m_chunkIDRanges.length * Long.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putShort(m_rangeID);
        p_buffer.putShort(m_creator);
        p_buffer.putShort(m_restorer);

        p_buffer.putInt(m_chunkIDRanges.length);
        for (long chunkID : m_chunkIDRanges) {
            p_buffer.putLong(chunkID);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_rangeID = p_buffer.getShort();
        m_creator = p_buffer.getShort();
        m_restorer = p_buffer.getShort();

        int size = p_buffer.getInt();
        m_chunkIDRanges = new long[size];
        for (int i = 0; i < size; i++) {
            m_chunkIDRanges[i] = p_buffer.getLong();
        }
    }

}
