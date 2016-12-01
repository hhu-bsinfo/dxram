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

/**
 * Update All Message
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.10.2015
 */
public class SetRestorerAfterRecoveryMessage extends AbstractMessage {

    // Attributes
    private short m_oldOwner;
    private short m_newOwner;

    // Constructors

    /**
     * Creates an instance of UpdateAllMessage
     */
    public SetRestorerAfterRecoveryMessage() {
        super();

        m_oldOwner = -1;
        m_newOwner = -1;
    }

    /**
     * Creates an instance of UpdateAllMessage
     *
     * @param p_destination
     *     the destination
     * @param p_oldOwner
     *     the failed peer
     * @param p_newOwner
     *     the new owner
     */
    public SetRestorerAfterRecoveryMessage(final short p_destination, final short p_oldOwner, final short p_newOwner) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_SET_RESTORER_AFTER_RECOVERY_MESSAGE);

        m_oldOwner = p_oldOwner;
        m_newOwner = p_newOwner;
    }

    // Getters

    /**
     * Get the old owner
     *
     * @return the NodeID
     */
    public final short getOldOwner() {
        return m_oldOwner;
    }

    /**
     * Get the new owner
     *
     * @return the NodeID
     */
    public final short getNewOwner() {
        return m_newOwner;
    }

    @Override
    protected final int getPayloadLength() {
        return 2 * Short.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putShort(m_oldOwner);
        p_buffer.putShort(m_newOwner);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_oldOwner = p_buffer.getShort();
        m_newOwner = p_buffer.getShort();
    }

}
