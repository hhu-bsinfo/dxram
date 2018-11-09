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
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Response to a GetRequest
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.12.2015
 */
public class GetMultiResponse extends Response {
    // total field used for reading, only
    private int m_total;
    // The data of the chunk objects here is used when sending the response only
    // when the response is received, the chunk objects from the request are
    // used to directly write the data to them to avoid further copying
    private ChunkByteArray[] m_dataChunks;

    /**
     * Creates an instance of GetResponse.
     * This constructor is used when receiving this message.
     */
    public GetMultiResponse() {
        super();
    }

    /**
     * Creates an instance of GetResponse.
     * This constructor is used when sending this message.
     * Make sure to include all data of the chunks from the request in the correct order. If a chunk does
     * not exist, set the byte[] at that index to null
     *
     * @param p_request
     *         the corresponding GetRequest
     * @param p_chunks
     *         Chunks read from the memory.
     */
    public GetMultiResponse(final GetMultiRequest p_request, final ChunkByteArray[] p_chunks) {
        super(p_request, ChunkMessages.SUBTYPE_GET_MULTI_RESPONSE);

        m_dataChunks = p_chunks;
    }

    @Override
    protected final int getPayloadLength() {
        int size = 0;

        // when writing payload
        if (m_dataChunks != null) {
            size += ObjectSizeUtil.sizeofCompactedNumber(m_dataChunks.length);

            size += m_dataChunks.length * Byte.BYTES;

            for (ChunkByteArray dataChunk : m_dataChunks) {
                if (dataChunk.getData() != null) {
                    size += dataChunk.getSize();
                }
            }
        } else {
            size += ObjectSizeUtil.sizeofCompactedNumber(m_total);

            // after reading message payload to request data structures
            GetMultiRequest request = (GetMultiRequest) getCorrespondingRequest();

            // chunk states
            size += request.getChunkCount() * Byte.BYTES;

            for (int i = 0; i < request.getLocationIndexBuffer().getSize(); i++) {
                if (request.getLocationIndexBuffer().get(i) == request.getTargetRemoteLocation()) {
                    AbstractChunk chunk = request.getChunks()[i];

                    if (chunk != null && chunk.getState() == ChunkState.OK) {
                        size += chunk.sizeofObject();
                    }
                }
            }
        }

        return size;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        // write total count once
        p_exporter.writeCompactNumber(m_dataChunks.length);

        for (int i = 0; i < m_dataChunks.length; i++) {
            p_exporter.writeByte((byte) m_dataChunks[i].getState().ordinal());

            if (m_dataChunks[i].isStateOk()) {
                p_exporter.writeBytes(m_dataChunks[i].getData());
            }
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_total = p_importer.readCompactNumber(m_total);

        // read the payload from the buffer and write it directly into
        // the chunk objects provided by the request to avoid further copying of data
        GetMultiRequest request = (GetMultiRequest) getCorrespondingRequest();

        if (request.getChunkCount() != m_total) {
            throw new IllegalStateException("Non matching chunk count on response compared to request: " +
                    request.getChunkCount() + " != " + m_total);
        }

        for (int i = 0; i < request.getLocationIndexBuffer().getSize(); i++) {
            if (request.getLocationIndexBuffer().get(i) == request.getTargetRemoteLocation()) {
                AbstractChunk chunk = request.getChunks()[i];

                chunk.setState(ChunkState.values()[p_importer.readByte((byte) chunk.getState().ordinal())]);

                if (chunk.getState() == ChunkState.OK) {
                    p_importer.importObject(chunk);
                }
            }
        }
    }
}
