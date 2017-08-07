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

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;

/**
 * Change the size of an existing barrier.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 06.05.2016
 */
public class BarrierChangeSizeRequest extends Request {
    private int m_barrierId;
    private int m_size;
    private boolean m_isReplicate;

    /**
     * Creates an instance of BarrierChangeSizeRequest
     */
    public BarrierChangeSizeRequest() {
        super();
    }

    /**
     * Creates an instance of BarrierChangeSizeRequest
     *
     * @param p_destination
     *         the destination
     * @param p_barrierId
     *         size of the barrier
     * @param p_size
     *         id of the barrier
     * @param p_isReplicate
     *         wether it is a replicate or not
     */
    public BarrierChangeSizeRequest(final short p_destination, final int p_barrierId, final int p_size, final boolean p_isReplicate) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_BARRIER_CHANGE_SIZE_REQUEST);

        m_barrierId = p_barrierId;
        m_size = p_size;
        m_isReplicate = p_isReplicate;
    }

    /**
     * Id of the barrier to change the size of.
     *
     * @return Barrier id
     */
    public int getBarrierId() {
        return m_barrierId;
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
        return Integer.BYTES + Integer.BYTES + Byte.BYTES;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_barrierId);
        p_exporter.writeInt(m_size);
        p_exporter.writeBoolean(m_isReplicate);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_barrierId = p_importer.readInt(m_barrierId);
        m_size = p_importer.readInt(m_size);
        m_isReplicate = p_importer.readBoolean(m_isReplicate);
    }
}
