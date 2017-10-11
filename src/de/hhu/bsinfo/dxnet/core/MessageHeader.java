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
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.07.2017
 */
public class MessageHeader implements Importable {

    /* Header size:
     *  messageID + type + subtype + messageType and exclusivity + payloadSize
     *  3b        + 1b   + 1b      + 1b                          + 4b           = 10 bytes
     */
    private int m_messageID;
    private byte m_messageTypeExc;
    private byte m_type;
    private byte m_subtype;
    private int m_payloadSize;

    // Just state for message creation and de-serialization, not part of actual header
    private volatile AbstractPipeIn m_pipeIn;
    private volatile UnfinishedImExporterOperation m_unfinishedOperation;
    private volatile long m_address;
    private volatile int m_currentPosition;
    private volatile int m_bytesAvailable;
    private volatile int m_slot;

    // Constructors

    /**
     * Creates an instance of MessageHeader
     */
    MessageHeader() {
    }

    @Override
    public String toString() {
        return "m_messageID " + m_messageID + ", m_messageTypeExc " + m_messageTypeExc + ", m_type " + m_type + ", m_subtype " + m_subtype +
                ", m_payloadSize " + m_payloadSize;
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
    public byte getType() {
        return m_type;
    }

    /**
     * Message subtype
     */
    public byte getSubtype() {
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
    public boolean isExclusive() {
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
        m_messageTypeExc = 0;
        m_type = 0;
        m_subtype = 0;
        m_payloadSize = 0;

        // State does not need to be cleared
    }

    /**
     * Set all message information for creating and de-serializing the message
     *
     * @param p_pipeIn
     *         the AbstractPipeIn
     * @param p_unfinishedOperation
     *         the unfinished operation
     * @param p_currentPosition
     *         the current position in buffer
     * @param p_address
     *         the buffer address
     * @param p_bytesAvailable
     *         the bytes available in buffer
     * @param p_slot
     *         the buffer slot in pipe
     */
    void setMessageInformation(final AbstractPipeIn p_pipeIn, final UnfinishedImExporterOperation p_unfinishedOperation, final long p_address,
            final int p_currentPosition, final int p_bytesAvailable, final int p_slot) {
        m_pipeIn = p_pipeIn;
        m_unfinishedOperation = p_unfinishedOperation;
        m_address = p_address;
        m_currentPosition = p_currentPosition;
        m_bytesAvailable = p_bytesAvailable;
        m_slot = p_slot;
    }

    /**
     * Create and de-serialize a new message
     *
     * @param p_importerCollection
     *         the importer collection
     * @return the completed message
     * @throws NetworkException
     *         it the message type/subtype is invalid
     */
    public Message createAndFillMessage(final MessageImporterCollection p_importerCollection, final LocalMessageHeaderPool p_messageHeaderPool)
            throws NetworkException {
        return m_pipeIn
                .createAndFillMessage(this, m_address, m_currentPosition, m_bytesAvailable, m_unfinishedOperation, p_importerCollection, p_messageHeaderPool,
                        m_slot);
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
