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
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierStatus;

/**
 * Message to release the signed on instances from the barrier.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.05.2016
 */
public class BarrierReleaseMessage extends Message {
    private int m_barrierId = -1;
    private BarrierStatus m_results;

    /**
     * Creates an instance of BarrierReleaseMessage.
     * This constructor is used when receiving this message.
     */
    public BarrierReleaseMessage() {
        super();
    }

    /**
     * Creates an instance of BarrierReleaseMessage.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *         the destination node id.
     * @param p_barrierId
     *         Id of the barrier that got released
     * @param p_barrierStatus
     *         Results of the barrier sign on process
     */
    public BarrierReleaseMessage(final short p_destination, final int p_barrierId,
            final BarrierStatus p_barrierStatus) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_BARRIER_RELEASE_MESSAGE);

        m_barrierId = p_barrierId;
        m_results = p_barrierStatus;
    }

    /**
     * Get the id of the barrier that got released
     *
     * @return Barrier id.
     */
    public int getBarrierId() {
        return m_barrierId;
    }

    /**
     * Get the barrier sign on results
     *
     * @return Barrier results
     */
    public BarrierStatus getBarrierResults() {
        return m_results;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + m_results.sizeofObject();
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_barrierId);
        p_exporter.exportObject(m_results);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_barrierId = p_importer.readInt(m_barrierId);
        if (m_results == null) {
            m_results = new BarrierStatus();
        }
        p_importer.importObject(m_results);
    }
}
