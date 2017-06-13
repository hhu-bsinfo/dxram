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

package de.hhu.bsinfo.dxram.ms.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService.StatusMaster;
import de.hhu.bsinfo.utils.serialization.ByteBufferImExporter;
import de.hhu.bsinfo.net.core.AbstractResponse;

/**
 * Response to the request to get the status of a master compute node.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class GetMasterStatusResponse extends AbstractResponse {
    private StatusMaster m_statusMaster;

    /**
     * Creates an instance of GetMasterStatusResponse.
     * This constructor is used when receiving this message.
     */
    public GetMasterStatusResponse() {
        super();
    }

    /**
     * Creates an instance of GetMasterStatusResponse.
     * This constructor is used when sending this message.
     * @param p_request
     *            request to respond to.
     * @param p_statusMaster
     *            Status data of the master to send back
     */
    public GetMasterStatusResponse(final GetMasterStatusRequest p_request, final StatusMaster p_statusMaster) {
        super(p_request, MasterSlaveMessages.SUBTYPE_GET_MASTER_STATUS_RESPONSE);

        m_statusMaster = p_statusMaster;
    }

    /**
     * Current status of the master.
     * @return Status of the master.
     */
    public StatusMaster getStatusMaster() {
        return m_statusMaster;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        ByteBufferImExporter exporter = new ByteBufferImExporter(p_buffer);

        if (m_statusMaster != null) {
            exporter.exportObject(m_statusMaster);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        ByteBufferImExporter importer = new ByteBufferImExporter(p_buffer);

        if (getStatusCode() == 0) {
            m_statusMaster = new StatusMaster();
            importer.importObject(m_statusMaster);
        }
    }

    @Override
    protected final int getPayloadLength() {
        if (m_statusMaster != null) {
            return m_statusMaster.sizeofObject();
        } else {
            return 0;
        }
    }
}
