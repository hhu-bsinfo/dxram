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

package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Start Recovery Message
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class StartRecoveryMessage extends AbstractMessage {

    // Attributes
    private short m_failedPeer;
    private long m_beginOfRange;

    // Constructors

    /**
     * Creates an instance of StartRecoveryMessage
     */
    public StartRecoveryMessage() {
        super();

        m_failedPeer = (short) -1;
        m_beginOfRange = -1;
    }

    /**
     * Creates an instance of StartRecoveryMessage
     *
     * @param p_destination
     *     the destination
     * @param p_failedPeer
     *     the failed peer
     * @param p_beginOfRange
     *     the beginning of the range that has to be recovered
     */
    public StartRecoveryMessage(final short p_destination, final short p_failedPeer, final int p_beginOfRange) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_START_RECOVERY_MESSAGE);

        assert p_failedPeer != NodeID.INVALID_ID;

        m_failedPeer = p_failedPeer;
        m_beginOfRange = p_beginOfRange;
    }

    // Getters

    /**
     * Get the failed peer
     *
     * @return the NodeID
     */
    public final short getFailedPeer() {
        return m_failedPeer;
    }

    /**
     * Get the beginning of range
     *
     * @return the beginning of the range that has to be recovered
     */
    public final long getBeginOfRange() {
        return m_beginOfRange;
    }

    @Override
    protected final int getPayloadLength() {
        return Short.BYTES + Long.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putShort(m_failedPeer);
        p_buffer.putLong(m_beginOfRange);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_failedPeer = p_buffer.getShort();
        m_beginOfRange = p_buffer.getLong();
    }

}
