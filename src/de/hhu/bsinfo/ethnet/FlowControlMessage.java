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

package de.hhu.bsinfo.ethnet;

import java.nio.ByteBuffer;

/**
 * Used to confirm received bytes
 *
 * @author Marc Ewert, marc.ewert@hhu.de, 14.10.2014
 */
class FlowControlMessage extends AbstractMessage {

    protected static final byte TYPE = 0;
    static final byte SUBTYPE = 1;

    private int m_confirmedBytes;

    /**
     * Default constructor for serialization
     */
    FlowControlMessage() {
    }

    /**
     * Create a new Message for confirming received bytes.
     *
     * @param p_confirmedBytes
     *     number of received bytes
     */
    FlowControlMessage(final int p_confirmedBytes) {
        super((short) 0, TYPE, SUBTYPE, true);
        m_confirmedBytes = p_confirmedBytes;
    }

    /**
     * Get number of confirmed bytes
     *
     * @return the number of confirmed bytes
     */
    int getConfirmedBytes() {
        return m_confirmedBytes;
    }

    @Override
    protected int getPayloadLength() {
        return 4;
    }

    @Override
    protected void readPayload(final ByteBuffer p_buffer) {
        m_confirmedBytes = p_buffer.getInt();
    }

    @Override
    protected void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putInt(m_confirmedBytes);
    }
}
