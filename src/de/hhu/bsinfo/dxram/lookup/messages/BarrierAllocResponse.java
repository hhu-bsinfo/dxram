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

import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.net.core.AbstractMessageImporter;
import de.hhu.bsinfo.net.core.Response;

/**
 * Response to the barrier alloc request
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.05.2016
 */
public class BarrierAllocResponse extends Response {
    private int m_barrierId;

    /**
     * Creates an instance of BarrierAllocResponse
     */
    public BarrierAllocResponse() {
        super();

    }

    /**
     * Creates an instance of BarrierAllocResponse
     *
     * @param p_request
     *         the corresponding BarrierAllocRequest
     * @param p_barrierId
     *         Id of the created barrier
     */
    public BarrierAllocResponse(final BarrierAllocRequest p_request, final int p_barrierId) {
        super(p_request, LookupMessages.SUBTYPE_BARRIER_ALLOC_RESPONSE);

        m_barrierId = p_barrierId;
    }

    /**
     * Get the id of the created barrier.
     *
     * @return Barrier id.
     */
    public int getBarrierId() {
        return m_barrierId;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_barrierId);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_barrierId = p_importer.readInt(m_barrierId);
    }
}
