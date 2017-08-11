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

package de.hhu.bsinfo.dxnet.core;

import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Used to import message headers.
 */
class MessageHeader implements Importable {

    /* Header size:
     *  messageID + type + subtype + messageType and exclusivity + payloadSize
     *  3b        + 1b   + 1b      + 1b                          + 4b           = 10 bytes
     */
    private int m_messageID;
    private byte m_messageTypeExc;
    private byte m_type;
    private byte m_subtype;
    private int m_payloadSize;

    // Constructors

    /**
     * Creates an instance of MessageHeader
     */
    MessageHeader() {
    }

    /**
     * ID of the message
     */
    int getMessageID() {
        return m_messageID;
    }

    /**
     * Message type
     */
    byte getType() {
        return m_type;
    }

    /**
     * Message subtype
     */
    byte getSubtype() {
        return m_subtype;
    }

    /**
     * Type of message (normal message or request)
     */
    byte getMessageType() {
        return (byte) (m_messageTypeExc >> 4);
    }

    /**
     * Check if message is exclusive
     */
    boolean isExclusive() {
        return (m_messageTypeExc & 0xF) == 1;
    }

    /**
     * Payload size of the message
     */
    int getPayloadSize() {
        return m_payloadSize;
    }

    /**
     * Clear message header attributes
     */
    void clear() {
        m_messageID = 0;
        m_type = 0;
        m_subtype = 0;
        m_payloadSize = 0;
    }

    @Override
    public void importObject(Importer p_importer) {
        // Read message ID (default 3 byte)
        int tmp;
        for (int i = 0; i < Message.MESSAGE_ID_LENGTH; i++) {
            tmp = p_importer.readByte((byte) 0);
            if (tmp != 0) {
                m_messageID |= (tmp & 0xFF) << (Message.MESSAGE_ID_LENGTH - 1 - i) * 8;
            }
        }

        m_type = p_importer.readByte(m_type);
        m_subtype = p_importer.readByte(m_subtype);
        m_messageTypeExc = p_importer.readByte(m_messageTypeExc);
        m_payloadSize = p_importer.readInt(m_payloadSize);
    }

    @Override
    public int sizeofObject() {
        return Message.HEADER_SIZE;
    }
}
