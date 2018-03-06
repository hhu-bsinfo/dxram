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

package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;

/**
 * Response to the sign on request with status code if successful.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.05.2016
 */
public class BarrierSignOnResponse extends Response {
    private int m_barrierIdentifier = -1;
    private byte m_status;

    /**
     * Creates an instance of SlaveSyncBarrierSignOnMessage.
     * This constructor is used when receiving this message.
     */
    public BarrierSignOnResponse() {
        super();
    }

    /**
     * Creates an instance of SlaveSyncBarrierSignOnMessage.
     * This constructor is used when sending this message.
     *
     * @param p_request
     *         the request to respond to.
     * @param p_status
     *         Status code of the sign on
     */
    public BarrierSignOnResponse(final BarrierSignOnRequest p_request, final byte p_status) {
        super(p_request, LookupMessages.SUBTYPE_BARRIER_SIGN_ON_RESPONSE);

        m_barrierIdentifier = p_request.getBarrierId();
        m_status = p_status;
    }

    /**
     * Id of the barrier
     *
     * @return Sync token.
     */
    public int getBarrierId() {
        return m_barrierIdentifier;
    }

    /**
     * Get the status
     *
     * @return the status
     */
    public byte getStatus() {
        return m_status;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + Byte.BYTES;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_barrierIdentifier);
        p_exporter.writeByte(m_status);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_barrierIdentifier = p_importer.readInt(m_barrierIdentifier);
        m_status = p_importer.readByte(m_status);
    }
}
