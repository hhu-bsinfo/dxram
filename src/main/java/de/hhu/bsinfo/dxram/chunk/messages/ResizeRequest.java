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

package de.hhu.bsinfo.dxram.chunk.messages;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Request for resizing a chunk on a remote node.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 04.09.2018
 */
public class ResizeRequest extends Request {
    private AbstractChunk[] m_chunks;
    // this is only used when receiving the request
    private long[] m_chunkIDs;
    private int[] m_newSizes;

    // state for deserialization
    private int m_tmpSize;

    /**
     * Creates an instance of ResizeRequest.
     * This constructor is used when receiving this message.
     */
    public ResizeRequest() {
        super();
    }

    /**
     * Creates an instance of GetRequest.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *         the destination node id.
     * @param p_chunks
     *         Chunks with the ID to resize
     */
    public ResizeRequest(final short p_destination, final AbstractChunk... p_chunks) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_RESIZE_REQUEST);

        m_chunks = p_chunks;
    }

    /**
     * Get the chunk IDs of this request (when receiving it).
     *
     * @return Chunk ID.
     */
    public long[] getChunkIDs() {
        return m_chunkIDs;
    }

    /**
     * Get the new sizes for resizing the chunks
     *
     * @return New sizes
     */
    public int[] getNewSizes() {
        return m_newSizes;
    }

    @Override
    protected final int getPayloadLength() {
        if (m_chunks != null) {
            return ObjectSizeUtil.sizeofCompactedNumber(m_chunks.length) +
                    (Integer.BYTES + Long.BYTES) * m_chunks.length;
        } else {
            return ObjectSizeUtil.sizeofCompactedNumber(m_chunkIDs.length) +
                    (Integer.BYTES + Long.BYTES) * m_chunkIDs.length;
        }
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeCompactNumber(m_chunks.length);

        for (AbstractChunk chunk : m_chunks) {
            p_exporter.writeLong(chunk.getID());
            p_exporter.writeInt(chunk.sizeofObject());
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_tmpSize = p_importer.readCompactNumber(m_tmpSize);

        if (m_chunkIDs == null) {
            m_chunkIDs = new long[m_tmpSize];
            m_newSizes = new int[m_tmpSize];
        }

        for (int i = 0; i < m_tmpSize; i++) {
            m_chunkIDs[i] = p_importer.readLong(m_chunkIDs[i]);
            m_newSizes[i] = p_importer.readInt(m_newSizes[i]);
        }
    }
}
