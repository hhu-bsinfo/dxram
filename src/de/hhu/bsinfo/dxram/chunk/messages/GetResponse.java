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

import java.nio.ByteBuffer;

import de.hhu.bsinfo.utils.serialization.ByteBufferImExporter;
import de.hhu.bsinfo.dxram.data.ChunkMessagesMetadataUtils;
import de.hhu.bsinfo.dxram.data.ChunkState;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to a GetRequest
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.12.2015
 */
public class GetResponse extends AbstractResponse {

    // The data of the chunk objects here is used when sending the response only
    // when the response is received, the chunk objects from the request are
    // used to directly write the data to them and avoiding further copying
    private byte[][] m_dataChunks;
    private int m_totalSuccessful;

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
     *     the corresponding GetRequest
     * @param p_dataChunks
     *     Array of byte arrays with chunk data read from the memory. If a chunk does not exist, the byte[] is null
     * @param p_totalSuccessful
     *     Number of total successful get operations
     */
    public GetResponse(final GetRequest p_request, final byte[][] p_dataChunks, final int p_totalSuccessful) {
        super(p_request, ChunkMessages.SUBTYPE_GET_RESPONSE);

        m_dataChunks = p_dataChunks;
        setStatusCode(ChunkMessagesMetadataUtils.setNumberOfItemsToSend(getStatusCode(), p_totalSuccessful));
    }

    /**
     * Get the total number of successful chunk gets
     *
     * @return Total number of successful chunk gets
     */
    public int getTotalSuccessful() {
        return m_totalSuccessful;
    }

    @Override
    protected final int getPayloadLength() {
        int size = ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode());

        // when writing payload
        if (m_dataChunks != null) {
            size += m_dataChunks.length * Byte.BYTES;

            for (int i = 0; i < m_dataChunks.length; i++) {
                if (m_dataChunks[i] != null) {
                    size += m_dataChunks[i].length;
                }
            }
        } else {
            // after reading message payload to request data structures
            GetRequest request = (GetRequest) getCorrespondingRequest();

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
    protected final void writePayload(final ByteBuffer p_buffer) {
        ChunkMessagesMetadataUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_totalSuccessful);

        for (int i = 0; i < m_dataChunks.length; i++) {
            if (m_dataChunks[i] == null) {
                // indicate no data available
                p_buffer.put((byte) 0);
            } else {
                p_buffer.put((byte) 1);
                p_buffer.put(m_dataChunks[i]);
            }
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_totalSuccessful = ChunkMessagesMetadataUtils.getNumberOfItemsFromMessageBuffer(getStatusCode(), p_buffer);

        // read the payload from the buffer and write it directly into
        // the chunk objects provided by the request to avoid further copying of data
        ByteBufferImExporter importer = new ByteBufferImExporter(p_buffer);
        GetRequest request = (GetRequest) getCorrespondingRequest();

        for (DataStructure chunk : request.getChunks()) {
            if (p_buffer.get() == 1) {
                importer.importObject(chunk);
                chunk.setState(ChunkState.OK);
            } else {
                chunk.setState(ChunkState.DOES_NOT_EXIST);
            }
        }
    }

}
