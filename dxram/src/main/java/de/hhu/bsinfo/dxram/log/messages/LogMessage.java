/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.log.messages;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.RangeID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxutils.ByteBufferHelper;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Message for logging a Chunk on a remote node
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 20.04.2016
 */
public class LogMessage extends Message {

    // Attributes
    private short m_rangeID;
    // For exporting
    private DataStructure[] m_dataStructures;
    // For importing
    private int m_numberOfDSs;
    private ByteBuffer m_buffer;

    // Constructors

    /**
     * Creates an instance of LogMessage
     */
    public LogMessage() {
        super();

        m_rangeID = RangeID.INVALID_ID;
        m_dataStructures = null;
        m_numberOfDSs = 0;
        m_buffer = null;
    }

    /**
     * Creates an instance of LogMessage
     *
     * @param p_destination
     *         the destination
     * @param p_dataStructures
     *         the data structures to store
     * @param p_rangeID
     *         the RangeID
     */
    public LogMessage(final short p_destination, final short p_rangeID, final DataStructure... p_dataStructures) {
        super(p_destination, DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_LOG_MESSAGE, true);

        m_rangeID = p_rangeID;
        m_dataStructures = p_dataStructures;
    }

    // Getters

    /**
     * Get the rangeID
     *
     * @return the rangeID
     */
    public final short getRangeID() {
        return m_rangeID;
    }

    /**
     * Get the number of data structures
     *
     * @return the number of data structures
     */
    public final int getNumberOfDataStructures() {
        return m_numberOfDSs;
    }

    /**
     * Get the message buffer
     *
     * @return the message buffer
     */
    public final ByteBuffer getMessageBuffer() {
        return m_buffer;
    }

    @Override
    protected final int getPayloadLength() {
        if (m_dataStructures != null) {
            int ret = Short.BYTES + Integer.BYTES;

            for (DataStructure dataStructure : m_dataStructures) {
                int size = dataStructure.sizeofObject();
                ret += Long.BYTES + ObjectSizeUtil.sizeofCompactedNumber(size) + size;
            }

            return ret;
        } else {
            return Short.BYTES + Integer.BYTES + m_buffer.limit();
        }
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeShort(m_rangeID);
        p_exporter.writeInt(m_dataStructures.length);

        for (DataStructure dataStructure : m_dataStructures) {
            final int size = dataStructure.sizeofObject();

            p_exporter.writeLong(dataStructure.getID());
            p_exporter.writeCompactNumber(size);
            p_exporter.exportObject(dataStructure);
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer, final int p_payloadSize) {
        m_rangeID = p_importer.readShort(m_rangeID);
        m_numberOfDSs = p_importer.readInt(m_numberOfDSs);

        // Just copy all bytes, will be serialized into primary write buffer later
        int payloadSize = p_payloadSize - Short.BYTES - Integer.BYTES;
        if (m_buffer == null) {
            m_buffer = ByteBuffer.allocateDirect(payloadSize);
            m_buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        p_importer.readBytes(ByteBufferHelper.getDirectAddress(m_buffer), 0, payloadSize);
    }

}
