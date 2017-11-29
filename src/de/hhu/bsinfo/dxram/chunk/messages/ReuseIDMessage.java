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
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxutils.ArrayListLong;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Message to reuse chunk IDs if migrated chunks are deleted.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 29.05.2017
 */
public class ReuseIDMessage extends Message {

    private ArrayListLong m_chunkIDsOut;
    private long[] m_chunkIDs;

    /**
     * Creates an instance of ReuseIDMessage.
     * This constructor is used when receiving this message.
     */
    public ReuseIDMessage() {
        super();
    }

    /**
     * Creates an instance of ReuseIDMessage.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *         the destination
     * @param p_chunkIds
     *         the chunk IDs to reuse
     */
    public ReuseIDMessage(final short p_destination, final ArrayListLong p_chunkIds) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_REUSE_ID_MESSAGE);
        m_chunkIDsOut = p_chunkIds;
    }

    /**
     * Get the Chunk IDs of deleted chunks to reuse
     *
     * @return the IDs to reuse
     */
    public final long[] getChunkIDs() {
        return m_chunkIDs;
    }

    @Override
    protected final int getPayloadLength() {
        int size = 0;

        if (m_chunkIDsOut != null) {
            size += ObjectSizeUtil.sizeofCompactedNumber(m_chunkIDsOut.getSize());
            size += Long.BYTES * m_chunkIDsOut.getSize();
        } else {
            size += ObjectSizeUtil.sizeofCompactedNumber(m_chunkIDs.length);
            size += Long.BYTES * m_chunkIDs.length;
        }

        return size;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeCompactNumber(m_chunkIDsOut.getSize());
        for (int i = 0; i < m_chunkIDsOut.getSize(); i++) {
            p_exporter.writeLong(m_chunkIDsOut.get(i));
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        int length = p_importer.readCompactNumber(0);
        if (m_chunkIDs == null) {
            // Do not overwrite existing array
            m_chunkIDs = new long[length];
        }
        for (int i = 0; i < m_chunkIDs.length; i++) {
            m_chunkIDs[i] = p_importer.readLong(m_chunkIDs[i]);
        }
    }

}
