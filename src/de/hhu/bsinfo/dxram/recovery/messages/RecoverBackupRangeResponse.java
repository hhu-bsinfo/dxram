/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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
    private int m_numberOfRecoveredChunks;

    // Constructors

    /**
     * Creates an instance of RecoverBackupRangeResponse
     */
    public RecoverBackupRangeResponse() {
        super();

        m_numberOfRecoveredChunks = 0;
    }

    /**
     * Creates an instance of RecoverBackupRangeResponse
     *
     * @param p_request
     *     the corresponding RecoverBackupRangeRequest
     * @param p_numberOfRecoveredChunks
     *     number of recovered chunks
     */
    public RecoverBackupRangeResponse(final RecoverBackupRangeRequest p_request, final int p_numberOfRecoveredChunks) {
        super(p_request, RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_RESPONSE);

        m_numberOfRecoveredChunks = p_numberOfRecoveredChunks;
    }

    // Getters

    /**
     * Returns the number of recovered chunks
     *
     * @return the number of recovered chunks
     */
    public final int getNumberOfRecoveredChunks() {
        return m_numberOfRecoveredChunks;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putInt(m_numberOfRecoveredChunks);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_numberOfRecoveredChunks = p_buffer.getInt();
    }

}
