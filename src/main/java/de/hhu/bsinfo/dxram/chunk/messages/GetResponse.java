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
public class GetResponse extends Response {
    // The data of the chunk object here is used when sending the response only
    // when the response is received, the chunk object from the request is
    // used to directly write the data to it to avoid further copying
    private ChunkByteArray m_dataChunk;

    /**
     * Creates an instance of GetResponse.
     * This constructor is used when receiving this message.
     */
    public GetResponse() {
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
     * @param p_chunk
     *         Chunk read from memory.
     */
    public GetResponse(final GetRequest p_request, final ChunkByteArray p_chunk) {
        super(p_request, ChunkMessages.SUBTYPE_GET_RESPONSE);

        m_dataChunk = p_chunk;
    }

    @Override
    protected final int getPayloadLength() {
        int size = 0;

        // when writing payload
        if (m_dataChunk != null) {
            size += Byte.BYTES;

            if (m_dataChunk.getData() != null) {
                size += m_dataChunk.getSize();
            }
        } else {
            // after reading message payload to request chunk
            GetRequest request = (GetRequest) getCorrespondingRequest();

            // chunk state
            size += Byte.BYTES;

            if (request.getChunk().getState() == ChunkState.OK) {
                size += request.getChunk().sizeofObject();
            }
        }

        return size;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeByte((byte) m_dataChunk.getState().ordinal());

        if (m_dataChunk.isStateOk()) {
            p_exporter.writeBytes(m_dataChunk.getData());
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        // read the payload from the buffer and write it directly into
        // the chunk objects provided by the request to avoid further copying of data
        GetRequest request = (GetRequest) getCorrespondingRequest();

        AbstractChunk chunk = request.getChunk();

        chunk.setState(ChunkState.values()[p_importer.readByte((byte) chunk.getState().ordinal())]);

        if (chunk.getState() == ChunkState.OK) {
            p_importer.importObject(chunk);
        }
    }
}
