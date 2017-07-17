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

package de.hhu.bsinfo.dxram.chunk.messages;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.net.core.AbstractMessage;
import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.net.core.AbstractMessageImporter;
import de.hhu.bsinfo.utils.serialization.ObjectSizeUtil;

/**
 * (Async) Message for updating a Chunk on a remote node
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.12.2015
 */
public class PutMessage extends AbstractMessage {

    // DataStructures used when sending the put message
    private DataStructure[] m_dataStructures;

    private byte m_lockCode;

    // Variables used when receiving the message
    private long[] m_chunkIDs;
    private byte[][] m_data;

    /**
     * Creates an instance of PutMessage.
     * This constructor is used when receiving this message.
     */
    public PutMessage() {
        super();
    }

    /**
     * Creates an instance of PutMessage
     *
     * @param p_destination
     *         the destination
     * @param p_unlockOperation
     *         if true a potential lock will be released
     * @param p_dataStructures
     *         Data structure with the data to put.
     */
    public PutMessage(final short p_destination, final ChunkLockOperation p_unlockOperation, final DataStructure... p_dataStructures) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_MESSAGE);

        m_dataStructures = p_dataStructures;
        switch (p_unlockOperation) {
            case NO_LOCK_OPERATION:
                m_lockCode = 0;
                break;
            case READ_LOCK:
                m_lockCode = 1;
                break;
            case WRITE_LOCK:
                m_lockCode = 2;
                break;
            default:
                break;
        }
    }

    /**
     * Get the chunk IDs of the data to put when this message is received.
     *
     * @return the IDs of the chunks to put
     */
    public long[] getChunkIDs() {
        return m_chunkIDs;
    }

    /**
     * Get the data of the chunks to put when this message is received
     *
     * @return Array of byte[] of chunk data to put
     */
    public byte[][] getChunkData() {
        return m_data;
    }

    /**
     * Get the unlock operation to execute after the put.
     *
     * @return Unlock operation.
     */
    public ChunkLockOperation getUnlockOperation() {
        if (m_lockCode == 0) {
            return ChunkLockOperation.NO_LOCK_OPERATION;
        } else if (m_lockCode == 1) {
            return ChunkLockOperation.READ_LOCK;
        } else {
            return ChunkLockOperation.WRITE_LOCK;
        }
    }

    @Override
    protected final int getPayloadLength() {
        int size = Byte.BYTES + ObjectSizeUtil.sizeofCompactedNumber(m_dataStructures.length);

        size += m_dataStructures.length * (Long.BYTES + Integer.BYTES);

        for (DataStructure dataStructure : m_dataStructures) {
            size += dataStructure.sizeofObject();
        }

        return size;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeByte(m_lockCode);
        p_exporter.writeCompactNumber(m_dataStructures.length);
        for (DataStructure dataStructure : m_dataStructures) {
            int size = dataStructure.sizeofObject();

            p_exporter.writeLong(dataStructure.getID());
            p_exporter.writeInt(size);
            p_exporter.exportObject(dataStructure);
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_lockCode = p_importer.readByte(m_lockCode);

        int length = p_importer.readCompactNumber(0);
        if (m_chunkIDs == null) {
            // Do not overwrite existing arrays
            m_chunkIDs = new long[length];
            m_data = new byte[length][];
        }

        for (int i = 0; i < m_dataStructures.length; i++) {
            m_chunkIDs[i] = p_importer.readLong(m_chunkIDs[i]);
            m_data[i] = p_importer.readByteArray(m_data[i]);
        }
    }
}
