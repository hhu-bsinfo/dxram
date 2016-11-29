/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response with status information about a remote chunk service.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 07.04.2016
 */
public class StatusResponse extends AbstractResponse {

    private ChunkService.Status m_status;

    /**
     * Creates an instance of StatusResponse.
     * This constructor is used when receiving this message.
     */
    public StatusResponse() {
        super();
    }

    /**
     * Creates an instance of StatusResponse.
     * This constructor is used when sending this message.
     *
     * @param p_request
     *     the corresponding StatusRequest
     * @param p_status
     *     the requested Status
     */
    public StatusResponse(final StatusRequest p_request, final ChunkService.Status p_status) {
        super(p_request, ChunkMessages.SUBTYPE_STATUS_RESPONSE);

        m_status = p_status;
    }

    /**
     * Get the chunk service status.
     *
     * @return Chunk service status.
     */
    public final ChunkService.Status getStatus() {
        return m_status;
    }

    @Override
    protected final int getPayloadLength() {
        return m_status.sizeofObject();
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);
        exporter.exportObject(m_status);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);
        m_status = new ChunkService.Status();
        importer.importObject(m_status);
    }
}
