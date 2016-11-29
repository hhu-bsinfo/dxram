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

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractMessage;

/**
 * Recover Message
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.10.2015
 */
public class RecoverMessage extends AbstractMessage {

    // Attributes
    private short m_owner;
    private boolean m_useLiveData;

    // Constructors

    /**
     * Creates an instance of RecoverMessage
     */
    public RecoverMessage() {
        super();

        m_owner = (short) -1;
    }

    /**
     * Creates an instance of RecoverMessage
     *
     * @param p_destination
     *     the destination
     * @param p_useLiveData
     *     whether the recover should use current logs or log files
     * @param p_owner
     *     the NodeID of the owner
     */
    public RecoverMessage(final short p_destination, final short p_owner, final boolean p_useLiveData) {
        super(p_destination, DXRAMMessageTypes.RECOVERY_MESSAGES_TYPE, RecoveryMessages.SUBTYPE_RECOVER_MESSAGE, true);

        m_owner = p_owner;
        m_useLiveData = p_useLiveData;
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

    @Override
    protected final int getPayloadLength() {
        return Short.BYTES + Byte.BYTES;
    }

    /**
     * Returns whether the recover should use current logs or log files
     *
     * @return whether the recover should use current logs or log files
     */
    public final boolean useLiveData() {
        return m_useLiveData;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putShort(m_owner);
        if (m_useLiveData) {
            p_buffer.put((byte) 1);
        } else {
            p_buffer.put((byte) 0);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_owner = p_buffer.getShort();

        final byte b = p_buffer.get();
        if (b == 1) {
            m_useLiveData = true;
        }
    }

}
