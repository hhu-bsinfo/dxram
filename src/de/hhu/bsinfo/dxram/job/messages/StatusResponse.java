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

package de.hhu.bsinfo.dxram.job.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.utils.serialization.ByteBufferImExporter;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to the status request to get information about remote job systems.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class StatusResponse extends AbstractResponse {
    private JobService.Status m_status;

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
     * @param p_request
     *            the corresponding StatusRequest
     * @param p_status
     *            the requested Status
     */
    public StatusResponse(final StatusRequest p_request, final JobService.Status p_status) {
        super(p_request, JobMessages.SUBTYPE_STATUS_RESPONSE);

        m_status = p_status;
    }

    /**
     * Get the job service status.
     * @return Job service status.
     */
    public final JobService.Status getStatus() {
        return m_status;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        ByteBufferImExporter exporter = new ByteBufferImExporter(p_buffer);
        exporter.exportObject(m_status);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        ByteBufferImExporter importer = new ByteBufferImExporter(p_buffer);
        importer.importObject(m_status);
    }

    @Override
    protected final int getPayloadLength() {
        return m_status.sizeofObject();
    }
}
