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
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;

/**
 * Request to allocate/create a new barrier.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.05.2016
 */
public class BarrierAllocRequest extends Request {
    private int m_size;
    private boolean m_isReplicate;

    /**
     * Creates an instance of BarrierAllocRequest
     */
    public BarrierAllocRequest() {
        super();
    }

    /**
     * Creates an instance of LookupRequest
     *
     * @param p_destination
     *         the destination
     * @param p_size
     *         size of the barrier
     * @param p_isReplicate
     *         wether it is a replicate or not
     */
    public BarrierAllocRequest(final short p_destination, final int p_size, final boolean p_isReplicate) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_BARRIER_ALLOC_REQUEST);

        m_size = p_size;
        m_isReplicate = p_isReplicate;
    }

    /**
     * Get the barrier size;
     *
     * @return Barrier size
     */
    public int getBarrierSize() {
        return m_size;
    }

    /**
     * Returns if it is a replicate or not.
     *
     * @return True if it is a replicate, false otherwise.
     */
    public boolean isReplicate() {
        return m_isReplicate;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + Byte.BYTES;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_size);
        p_exporter.writeBoolean(m_isReplicate);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_size = p_importer.readInt(m_size);
        m_isReplicate = p_importer.readBoolean(m_isReplicate);
    }
}
