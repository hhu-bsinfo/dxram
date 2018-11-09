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

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Response to a LockRequest
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 04.09.2018
 */
public class LockResponse extends Response {
    private byte[] m_chunkStatusCodes;

    /**
     * Creates an instance of LockResponse.
     * This constructor is used when receiving this message.
     */
    public LockResponse() {
        super();
    }

    /**
     * Creates an instance of LockResponse.
     * This constructor is used when sending this message.
     *
     * @param p_request
     *         the request
     * @param p_statusCodes
     *         Status code for every lock operation
     */
    public LockResponse(final LockRequest p_request, final byte... p_statusCodes) {
        super(p_request, ChunkMessages.SUBTYPE_LOCK_RESPONSE);
        m_chunkStatusCodes = p_statusCodes;
    }

    /**
     * Get the status codes
     */
    public final byte[] getStatusCodes() {
        return m_chunkStatusCodes;
    }

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofByteArray(m_chunkStatusCodes);
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeByteArray(m_chunkStatusCodes);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_chunkStatusCodes = p_importer.readByteArray(m_chunkStatusCodes);
    }
}
