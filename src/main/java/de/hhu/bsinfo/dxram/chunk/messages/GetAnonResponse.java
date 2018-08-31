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

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxram.chunk.data.ChunkAnon;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Response to a GetAnonRequest
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 30.03.2017
 */
public class GetAnonResponse extends Response {
    // total field used for reading, only
    private int m_total;
    // The data of the chunk objects here is used when sending the response only
    // when the response is received, the chunk objects from the request are
    // used to directly write the data to them to avoid further copying
    private ChunkByteArray[] m_dataChunks;

    /**
     * Creates an instance of GetAnonResponse.
     * This constructor is used when receiving this message.
     */
    public GetAnonResponse() {
        super();
    }

    /**
     * Creates an instance of GetAnonResponse.
     * This constructor is used when sending this message.
     * Make sure to include all data of the chunks from the request in the correct order. If a chunk does
     * not exist, set the byte[] at that index to null
     *
     * @param p_request
     *         the corresponding GetRequest
     * @param p_chunks
     *         Chunk data read from the memory.
     */
    public GetAnonResponse(final GetAnonRequest p_request, final ChunkByteArray[] p_chunks) {
        super(p_request, ChunkMessages.SUBTYPE_GET_ANON_RESPONSE);

        m_dataChunks = p_chunks;
    }

    @Override
    protected final int getPayloadLength() {
        // write total count once
        int size = 0;

        // when writing payload
        if (m_dataChunks != null) {
            size += ObjectSizeUtil.sizeofCompactedNumber(m_dataChunks.length);
            size += m_dataChunks.length * Byte.BYTES;

            for (ChunkByteArray dataChunk : m_dataChunks) {
                size += ObjectSizeUtil.sizeofByteArray(dataChunk.getData());
            }
        } else {
            // after reading message payload to request data structures
            GetAnonRequest request = (GetAnonRequest) getCorrespondingRequest();

            size += ObjectSizeUtil.sizeofCompactedNumber(m_total);

            // chunk states
            size += request.getChunks().length * Byte.BYTES;

            for (int i = 0; i < request.getChunks().length; i++) {
                if (request.getChunks()[i] != null && request.getChunks()[i].getState() == ChunkState.OK) {
                    size += request.getChunks()[i].sizeofObject();
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
                // write byte array with length information
                p_exporter.writeByteArray(m_dataChunks[i].getData());
            }
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_total = p_importer.readCompactNumber(m_total);

        // read the payload from the buffer and write it directly into
        // the chunk objects provided by the request to avoid further copying of data
        GetAnonRequest request = (GetAnonRequest) getCorrespondingRequest();

        for (int i = 0; i < m_total; i++) {
            ChunkAnon chunk = request.getChunks()[i];

            chunk.setState(ChunkState.values()[p_importer.readByte((byte) chunk.getState().ordinal())]);

            if (chunk.getState() == ChunkState.OK) {
                p_importer.importObject(chunk);
            }
        }
    }
}
