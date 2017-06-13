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
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.net.core.AbstractRequest;

/**
 * Request to create new chunks remotely.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 13.01.2016
 */
public class CreateRequest extends AbstractRequest {
    private int[] m_sizes;

    /**
     * Creates an instance of CreateRequest.
     * This constructor is used when receiving this message.
     */
    public CreateRequest() {
        super();
    }

    /**
     * Creates an instance of CreateRequest.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *     the destination node id.
     * @param p_sizes
     *     Sizes of the chunks to create.
     */
    public CreateRequest(final short p_destination, final int... p_sizes) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_CREATE_REQUEST);

        m_sizes = p_sizes;

        setStatusCode(ChunkMessagesMetadataUtils.setNumberOfItemsToSend(getStatusCode(), p_sizes.length));
    }

    /**
     * Get the sizes received.
     *
     * @return Array of sizes to create chunks of.
     */
    public int[] getSizes() {
        return m_sizes;
    }

    @Override
    protected final int getPayloadLength() {
        return ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode()) + Integer.BYTES * m_sizes.length;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        ChunkMessagesMetadataUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_sizes.length);

        for (int size : m_sizes) {
            p_buffer.putInt(size);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        int numSizes = ChunkMessagesMetadataUtils.getNumberOfItemsFromMessageBuffer(getStatusCode(), p_buffer);

        m_sizes = new int[numSizes];
        for (int i = 0; i < m_sizes.length; i++) {
            m_sizes[i] = p_buffer.getInt();
        }
    }
}
