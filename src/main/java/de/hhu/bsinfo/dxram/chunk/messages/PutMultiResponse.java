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
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Response to a PutMultiRequest
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.12.2015
 */
public class PutMultiResponse extends Response {
    // sender only
    private byte[] m_chunkStatusCodes;

    // receiver only
    private int m_totalStatusCodeCount;

    /**
     * Creates an instance of PutMultiResponse.
     * This constructor is used when receiving this message.
     */
    public PutMultiResponse() {
        super();
    }

    /**
     * Creates an instance of PutMultiResponse.
     * This constructor is used when sending this message.
     *
     * @param p_request
     *         the request
     * @param p_statusCodes
     *         Status code for every single chunk put.
     */
    public PutMultiResponse(final PutMultiRequest p_request, final byte... p_statusCodes) {
        super(p_request, ChunkMessages.SUBTYPE_PUT_MULTI_RESPONSE);
        m_chunkStatusCodes = p_statusCodes;
    }

    @Override
    protected final int getPayloadLength() {
        if (m_chunkStatusCodes != null) {
            // sender side
            return Byte.BYTES * m_chunkStatusCodes.length;
        } else {
            // receiver side
            return Byte.BYTES * m_totalStatusCodeCount;
        }
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeBytes(m_chunkStatusCodes);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        PutMultiRequest request = (PutMultiRequest) getCorrespondingRequest();

        for (int i = 0; i < request.getLocationIndexBuffer().getSize(); i++) {
            if (request.getLocationIndexBuffer().get(i) == request.getTargetRemoteLocation()) {
                AbstractChunk chunk = request.getChunks()[i];

                chunk.setState(ChunkState.values()[p_importer.readByte((byte) chunk.getState().ordinal())]);

                m_totalStatusCodeCount++;
            }
        }
    }

}
