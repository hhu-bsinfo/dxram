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

package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.utils.serialization.ByteBufferImExporter;
import de.hhu.bsinfo.dxram.data.ChunkState;
import de.hhu.bsinfo.ethnet.AbstractResponse;
import de.hhu.bsinfo.utils.serialization.ObjectSizeUtil;

/**
 * Response to the get request.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 04.042017
 */
public class SuperpeerStorageGetAnonResponse extends AbstractResponse {
    // The data of the chunk buffer object here is used when sending the response only
    // when the response is received, the chunk objects from the request are
    // used to directly write the data to it to avoid further copying
    private byte[] m_data;

    /**
     * Creates an instance of SuperpeerStorageGetAnonResponse.
     * This constructor is used when receiving this message.
     */
    public SuperpeerStorageGetAnonResponse() {
        super();
    }

    /**
     * Creates an instance of SuperpeerStorageGetAnonResponse.
     * This constructor is used when sending this message.
     *
     * @param p_request
     *     the corresponding GetRequest
     * @param p_data
     *     Data read from memory
     */
    public SuperpeerStorageGetAnonResponse(final SuperpeerStorageGetAnonRequest p_request, final byte[] p_data) {
        super(p_request, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_GET_ANON_RESPONSE);

        m_data = p_data;
    }

    @Override
    protected final int getPayloadLength() {
        if (m_data != null) {
            return Byte.BYTES + ObjectSizeUtil.sizeofByteArray(m_data);
        } else {
            SuperpeerStorageGetAnonRequest request = (SuperpeerStorageGetAnonRequest) getCorrespondingRequest();
            return Byte.BYTES + request.getDataStructure().sizeofObject();
        }
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        if (m_data == null) {
            // indicate no data available
            p_buffer.put((byte) 0);
        } else {
            p_buffer.put((byte) 1);
            p_buffer.putInt(m_data.length);
            p_buffer.put(m_data);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        // read the payload from the buffer and write it directly into
        // the data structure provided by the request to avoid further copying of data
        ByteBufferImExporter importer = new ByteBufferImExporter(p_buffer);
        SuperpeerStorageGetAnonRequest request = (SuperpeerStorageGetAnonRequest) getCorrespondingRequest();

        if (p_buffer.get() == 1) {
            importer.importObject(request.getDataStructure());
            request.getDataStructure().setState(ChunkState.OK);
        } else {
            request.getDataStructure().setState(ChunkState.DOES_NOT_EXIST);
        }
    }
}
