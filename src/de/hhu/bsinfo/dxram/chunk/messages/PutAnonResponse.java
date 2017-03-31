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

import de.hhu.bsinfo.dxram.data.ChunkMessagesMetadataUtils;
import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to a PutAnonRequest
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 30.03.2017
 */
public class PutAnonResponse extends AbstractResponse {
    private byte[] m_chunkStatusCodes;

    /**
     * Creates an instance of PutAnonResponse.
     * This constructor is used when receiving this message.
     */
    public PutAnonResponse() {
        super();
    }

    /**
     * Creates an instance of PutAnonResponse.
     * This constructor is used when sending this message.
     *
     * @param p_request
     *     the request
     * @param p_statusCodes
     *     Status code for every single chunk put.
     */
    public PutAnonResponse(final PutAnonRequest p_request, final byte... p_statusCodes) {
        super(p_request, ChunkMessages.SUBTYPE_PUT_BUFFER_RESPONSE);

        m_chunkStatusCodes = p_statusCodes;

        setStatusCode(ChunkMessagesMetadataUtils.setNumberOfItemsToSend(getStatusCode(), p_statusCodes.length));
    }

    /**
     * Get the status
     *
     * @return true if put was successful
     */
    public final byte[] getStatusCodes() {
        return m_chunkStatusCodes;
    }

    @Override
    protected final int getPayloadLength() {
        return ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode()) + m_chunkStatusCodes.length * Byte.BYTES;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        ChunkMessagesMetadataUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_chunkStatusCodes.length);

        p_buffer.put(m_chunkStatusCodes);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        int numChunks = ChunkMessagesMetadataUtils.getNumberOfItemsFromMessageBuffer(getStatusCode(), p_buffer);

        m_chunkStatusCodes = new byte[numChunks];

        p_buffer.get(m_chunkStatusCodes);
    }

}
