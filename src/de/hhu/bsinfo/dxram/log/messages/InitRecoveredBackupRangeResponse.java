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

package de.hhu.bsinfo.dxram.log.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.net.core.AbstractResponse;

/**
 * Response to a InitBackupRangeRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 20.04.2016
 */
public class InitRecoveredBackupRangeResponse extends AbstractResponse {

    // Attributes
    private boolean m_success;

    // Constructors

    /**
     * Creates an instance of InitBackupRangeResponse
     */
    public InitRecoveredBackupRangeResponse() {
        super();

        m_success = false;
    }

    /**
     * Creates an instance of InitBackupRangeResponse
     *
     * @param p_request
     *     the request
     * @param p_success
     *     true if remove was successful
     */
    public InitRecoveredBackupRangeResponse(final InitRecoveredBackupRangeRequest p_request, final boolean p_success) {
        super(p_request, LogMessages.SUBTYPE_INIT_RECOVERED_BACKUP_RANGE_RESPONSE);

        m_success = p_success;
    }

    // Getters

    /**
     * Get the status
     *
     * @return true if remove was successful
     */
    public final boolean getStatus() {
        return m_success;
    }

    @Override
    protected final int getPayloadLength() {
        return Byte.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        if (m_success) {
            p_buffer.put((byte) 1);
        } else {
            p_buffer.put((byte) 0);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        final byte b = p_buffer.get();
        m_success = b == 1;
    }

}
