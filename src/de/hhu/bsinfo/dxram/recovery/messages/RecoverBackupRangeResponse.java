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

package de.hhu.bsinfo.dxram.recovery.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to a RecoverBackupRangeRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 08.10.2015
 */
public class RecoverBackupRangeResponse extends AbstractResponse {

    // Attributes
    private int m_numberOfChunks;
    private long[] m_chunkIDRanges;

    // Constructors

    /**
     * Creates an instance of RecoverBackupRangeResponse
     */
    public RecoverBackupRangeResponse() {
        super();

        m_numberOfChunks = 0;
        m_chunkIDRanges = null;
    }

    /**
     * Creates an instance of RecoverBackupRangeResponse
     *
     * @param p_request
     *     the corresponding RecoverBackupRangeRequest
     * @param p_numberOfChunks
     *     the number of recovered chunks
     * @param p_chunkIDRanges
     *     all ChunkIDs in ranges
     */
    public RecoverBackupRangeResponse(final RecoverBackupRangeRequest p_request, final int p_numberOfChunks, final long[] p_chunkIDRanges) {
        super(p_request, RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_RESPONSE);

        m_numberOfChunks = p_numberOfChunks;
        m_chunkIDRanges = p_chunkIDRanges;
    }

    // Getters

    /**
     * Returns the number of recovered chunks
     *
     * @return the number of recovered chunks
     */
    public final int getNumberOfChunks() {
        return m_numberOfChunks;
    }

    /**
     * Returns the ChunkIDs of all recovered chunks arranged in ranges
     *
     * @return the new backup peer
     */
    public final long[] getChunkIDRanges() {
        return m_chunkIDRanges;
    }

    @Override
    protected final int getPayloadLength() {
        if (m_numberOfChunks > 0) {
            return 2 * Integer.BYTES + m_chunkIDRanges.length * Long.BYTES;
        } else {
            return Integer.BYTES;
        }
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putInt(m_numberOfChunks);

        if (m_numberOfChunks > 0) {
            p_buffer.putInt(m_chunkIDRanges.length);
            for (int i = 0; i < m_chunkIDRanges.length; i++) {
                p_buffer.putLong(m_chunkIDRanges[i]);
            }
        }

    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_numberOfChunks = p_buffer.getInt();

        if (m_numberOfChunks > 0) {
            int size = p_buffer.getInt();
            m_chunkIDRanges = new long[size];
            for (int i = 0; i < size; i++) {
                m_chunkIDRanges[i] = p_buffer.getLong();
            }
        }
    }

}
