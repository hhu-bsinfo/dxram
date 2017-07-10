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

import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierStatus;
import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.net.core.AbstractMessageImporter;
import de.hhu.bsinfo.net.core.AbstractResponse;

/**
 * Message to get the current status of an active barrier.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 06.05.2016
 */
public class BarrierGetStatusResponse extends AbstractResponse {
    private int m_barrierId;
    private BarrierStatus m_barrierStatus;

    /**
     * Creates an instance of BarrierGetStatusResponse.
     * This constructor is used when receiving this message.
     */
    public BarrierGetStatusResponse() {
        super();
    }

    /**
     * Creates an instance of BarrierGetStatusResponse.
     * This constructor is used when sending this message.
     *
     * @param p_request
     *         The request for the response
     * @param p_barrierStatus
     *         Status of the barrier
     */
    public BarrierGetStatusResponse(final BarrierGetStatusRequest p_request, final BarrierStatus p_barrierStatus) {
        super(p_request, LookupMessages.SUBTYPE_BARRIER_STATUS_RESPONSE);

        m_barrierId = p_request.getBarrierId();
        m_barrierStatus = p_barrierStatus;
    }

    /**
     * Get the id of the barrier
     *
     * @return Barrier id.
     */
    public int getBarrierId() {
        return m_barrierId;
    }

    /**
     * Get the barrier status..
     *
     * @return Barrier status.
     */
    public BarrierStatus getBarrierStatus() {
        return m_barrierStatus;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + m_barrierStatus.sizeofObject();
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_barrierId);
        p_exporter.exportObject(m_barrierStatus);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_barrierId = p_importer.readInt();
        m_barrierStatus = new BarrierStatus();
        p_importer.importObject(m_barrierStatus);
    }
}
