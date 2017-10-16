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

import java.nio.BufferOverflowException;
import java.util.concurrent.atomic.AtomicInteger;

import de.hhu.bsinfo.utils.NodeID;

/**
 * Represents a network message
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 * @author Marc Ewert, marc.ewert@hhu.de, 18.09.2014
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.08.2017
 */
public class Message {

    static final boolean DEFAULT_EXCLUSIVITY_VALUE = false;
    /*- Header size:
     *  messageID + type + subtype + messageType and exclusivity + payloadSize
     *  3b        + 1b   + 1b      + 1b                          + 4b           = 10 bytes
     */
    static final byte HEADER_SIZE = 10;
    static final byte MESSAGE_ID_LENGTH = 3;
    // Constants
    private static final int INVALID_MESSAGE_ID = -1;
    private static final byte DEFAULT_TYPE = 0;
    private static final byte DEFAULT_SUBTYPE = 0;

    private static AtomicInteger ms_nextMessageID = new AtomicInteger();

    // Attributes
    // (!) MessageID occupies only 3 byte in message header
    private int m_messageID = INVALID_MESSAGE_ID;
    private short m_source;
    private short m_destination;
    // Message type: message and requests -> 0, responses -> 1; used to avoid instanceof in message processing
    private byte m_messageType;
    private byte m_type;
    private byte m_subtype;
    // (!) Exclusivity is written as a byte (0 -> false, 1 -> true)
    private boolean m_exclusivity;

    private int m_oldMessageID = INVALID_MESSAGE_ID;
    private volatile long m_sendReceiveTimestamp;

    /**
     * Creates an instance of Message
     *
     * @param p_destination
     *         the destination
     * @param p_type
     *         the message type
     * @param p_subtype
     *         the message subtype
     */
    protected Message(final short p_destination, final byte p_type, final byte p_subtype) {
        this(getNextMessageID(), p_destination, p_type, p_subtype, DEFAULT_EXCLUSIVITY_VALUE);
    }

    /**
     * Creates an instance of Message
     *
     * @param p_destination
     *         the destination
     * @param p_type
     *         the message type
     * @param p_subtype
     *         the message subtype
     * @param p_exclusivity
     *         whether this message type allows parallel execution
     */
    protected Message(final short p_destination, final byte p_type, final byte p_subtype, final boolean p_exclusivity) {
        this(getNextMessageID(), p_destination, p_type, p_subtype, p_exclusivity);
    }

    /**
     * Creates an instance of Message
     */
    protected Message() {
        m_messageID = INVALID_MESSAGE_ID;
        m_source = NodeID.INVALID_ID;
        m_destination = NodeID.INVALID_ID;
        m_messageType = 0;
        m_type = DEFAULT_TYPE;
        m_subtype = DEFAULT_SUBTYPE;

        m_exclusivity = DEFAULT_EXCLUSIVITY_VALUE;
    }

    /**
     * Creates an instance of Message
     *
     * @param p_messageID
     *         the messageID
     * @param p_destination
     *         the destination
     * @param p_type
     *         the message type
     * @param p_subtype
     *         the message subtype
     */
    protected Message(final int p_messageID, final short p_destination, final byte p_type, final byte p_subtype) {
        this(p_messageID, p_destination, p_type, p_subtype, DEFAULT_EXCLUSIVITY_VALUE);

        // Set message type to 1 for responses only
        m_messageType = (byte) 1;
    }

    /**
     * Creates an instance of Message
     *
     * @param p_messageID
     *         the messageID
     * @param p_destination
     *         the destination
     * @param p_type
     *         the message type
     * @param p_subtype
     *         the message subtype
     * @param p_exclusivity
     *         whether this is an exclusive message or not
     */
    private Message(final int p_messageID, final short p_destination, final byte p_type, final byte p_subtype, final boolean p_exclusivity) {
        assert p_destination != NodeID.INVALID_ID;

        m_messageID = p_messageID;
        m_source = -1;
        m_destination = p_destination;
        m_messageType = 0;
        m_type = p_type;
        m_subtype = p_subtype;

        m_exclusivity = p_exclusivity;
    }

    /**
     * (Re-) initialize the message with a (new) header
     *
     * @param p_header
     *         Message header to apply to the message
     */
    void initialize(final MessageHeader p_header, final short p_destination, final short p_source) {
        m_messageID = p_header.getMessageID();
        m_type = p_header.getType();
        m_subtype = p_header.getSubtype();
        m_messageType = p_header.getMessageType();
        m_exclusivity = p_header.isExclusive();
        m_destination = p_destination;
        m_source = p_source;
    }

    /**
     * Get the source
     *
     * @return the source
     */
    public final short getSource() {
        return m_source;
    }

    /**
     * Get the destination
     *
     * @return the destination
     */
    public final short getDestination() {
        return m_destination;
    }

    /**
     * Return if this message is a response
     *
     * @return true if message is a response, false otherwise (message or request)
     */
    public final boolean isResponse() {
        return m_messageType == 1;
    }

    /**
     * Get the message type
     *
     * @return the message type
     */
    public final byte getType() {
        return m_type;
    }

    /**
     * Get the message subtype
     *
     * @return the message subtype
     */
    public final byte getSubtype() {
        return m_subtype;
    }

    /**
     * Returns whether this message type allows parallel execution
     *
     * @return the exclusivity
     */
    public final boolean isExclusive() {
        return m_exclusivity;
    }

    public final void setDestination(final short p_destination) {
        m_destination = p_destination;
    }

    /**
     * Serialize the message using the provided exporter
     *
     * @param p_exporter
     *         the AbstractMessageExporter to export message with
     * @param p_messageSize
     *         the size of the message to serialize
     * @throws NetworkException
     *         if message could not be serialized
     */
    public final void serialize(final AbstractMessageExporter p_exporter, final int p_messageSize) throws NetworkException {
        writeMessage(p_exporter, p_messageSize - HEADER_SIZE);
    }

    @Override
    public final String toString() {
        if (m_source != -1) {
            return getClass().getSimpleName() + '[' + m_messageID + ", " + NodeID.toHexString(m_source) + ", " + NodeID.toHexString(m_destination) + ']';
        } else {
            return getClass().getSimpleName() + '[' + m_messageID + ", " + NodeID.toHexString(m_destination) + ']';
        }
    }

    /**
     * Get the total size of the message (header + payload)
     */
    int getTotalSize() {
        return HEADER_SIZE + getPayloadLength();
    }

    /**
     * Set the timestamp when the message is considered sent or received
     */
    void setSendReceiveTimestamp(final long p_timestampNs) {
        m_sendReceiveTimestamp = p_timestampNs;
    }

    /**
     * Get the timestamp when the message was sent or received
     */
    long getSendReceiveTimestamp() {
        return m_sendReceiveTimestamp;
    }

    /**
     * Get the total number of bytes the payload requires to create a buffer.
     *
     * @return Number of bytes of the payload
     */
    protected int getPayloadLength() {
        return 0;
    }

    /**
     * Reads the message payload
     * This method might be interrupted on every operation as payload can be scattered over several packets (this is always
     * the case for messages larger than network buffer size). As a consequence, this method might be called several times
     * for one single message. Thus, every operation in overwritten methods must be idempotent (same result for repeated
     * execution). All available import methods from importer guarantee idempotence and work atomically (read all or nothing).
     * Spare other I/O accesses and prints.
     * Example implementation for data structures (importable, exportable objects):
     * if (m_obj == null) {
     * m_obj = new ImExObject();
     * }
     * p_importer.importObject(m_obj);
     * Example implementation for array lists:
     * m_size = p_importer.readInt(m_size);
     * if (m_arrayList == null) {
     * // Do not overwrite array list after overflow
     * m_arrayList = new ArrayList<>(m_size);
     * }
     * for (int i = 0; i < m_size; i++) {
     * long l = p_importer.readLong(0);
     * if (m_arrayList.size() == i) {
     * m_arrayList.add(l);
     * }
     * }
     *
     * @param p_importer
     *         the importer
     */
    // @formatter:on
    protected void readPayload(final AbstractMessageImporter p_importer) {
    }

    /**
     * Reads the message payload; used for logging to copy directly into primary write buffer if possible
     *
     * @param p_importer
     *         the importer
     * @param p_payloadSize
     *         the message's payload size
     */
    protected void readPayload(final AbstractMessageImporter p_importer, final int p_payloadSize) {
        readPayload(p_importer);
    }

    /**
     * Writes the message payload
     *
     * @param p_exporter
     *         the buffer
     */
    protected void writePayload(final AbstractMessageExporter p_exporter) {
    }

    /**
     * Get the messageID
     *
     * @return the messageID
     */
    final int getMessageID() {
        return m_messageID;
    }

    /**
     * Get next free messageID
     *
     * @return next free messageID
     */
    private static int getNextMessageID() {
        return ms_nextMessageID.incrementAndGet();
    }

    /**
     * Reset all state and assign new message ID.
     */
    public void reuse() {
        // Message reused (probably pooled)
        if (m_messageID == m_oldMessageID) {
            m_messageID = getNextMessageID();
        }
    }

    /**
     * Re-initialize message attributes if response is reused
     *
     * @param p_messageID
     *         the message ID
     * @param p_destination
     *         the destination
     * @param p_type
     *         the type
     * @param p_subtype
     *         the subtype
     */
    public void set(final int p_messageID, final short p_destination, final byte p_type, final byte p_subtype) {
        m_messageID = p_messageID;
        m_source = -1;
        m_destination = p_destination;
        m_type = p_type;
        m_subtype = p_subtype;
        m_exclusivity = DEFAULT_EXCLUSIVITY_VALUE;

        // Set message type to 1 for responses only
        m_messageType = (byte) 1;
    }

    /**
     * Write the message using the provided exporter
     *
     * @param p_exporter
     *         the AbstractMessageExporter to export message with
     * @param p_payloadSize
     *         the payload size of the message
     * @throws NetworkException
     *         If writing the message failed
     */
    private void writeMessage(final AbstractMessageExporter p_exporter, final int p_payloadSize) throws NetworkException {
        try {
            // Message reused (probably pooled)
            if (m_messageID == m_oldMessageID) {
                m_messageID = getNextMessageID();
            }

            // Put message ID (default 3 byte)
            for (int i = 0; i < Message.MESSAGE_ID_LENGTH; i++) {
                p_exporter.writeByte((byte) (m_messageID >> (Message.MESSAGE_ID_LENGTH - 1 - i) * 8 & 0xFF));
            }
            p_exporter.writeByte(m_type);
            p_exporter.writeByte(m_subtype);
            p_exporter.writeByte((byte) ((m_messageType << 4) + (m_exclusivity ? 1 : 0)));
            p_exporter.writeInt(p_payloadSize);

            writePayload(p_exporter);
        } catch (final BufferOverflowException e) {
            throw new NetworkException(
                    "Could not create message " + this + ", because message buffer is too small, payload size " + p_payloadSize + "\nExporterState:\n" +
                            p_exporter, e);
        }

        int numberOfWrittenBytes = p_exporter.getNumberOfWrittenBytes();
        int messageSize = p_payloadSize + HEADER_SIZE;
        if (numberOfWrittenBytes < messageSize) {
            throw new NetworkException(
                    "Did not create message " + this + ", because message contents are smaller than expected payload size: " + numberOfWrittenBytes + " < " +
                            messageSize + "\nExporterState:\n" + p_exporter);
        }

        m_oldMessageID = m_messageID;
    }

}
