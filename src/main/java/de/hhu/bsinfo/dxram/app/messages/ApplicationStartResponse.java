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

package de.hhu.bsinfo.dxram.app.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Response to starting an application on a remote peer
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.10.2018
 */
public class ApplicationStartResponse extends Response {
    private boolean m_success;

    /**
     * Creates an instance of ApplicationStartResponse.
     * This constructor is used when receiving this message.
     */
    public ApplicationStartResponse() {
        super();
    }

    /**
     * Creates an instance of ApplicationStartResponse.
     * This constructor is used when sending this message.
     *
     * @param p_request
     *         Corresponding request
     */
    public ApplicationStartResponse(final ApplicationStartRequest p_request, final boolean p_success) {
        super(p_request, ApplicationMessages.SUBTYPE_START_APPLICATION_RESPONSE);

        m_success = p_success;
    }

    /**
     * Check if starting the application was successful
     *
     * @return True on success, false on error
     */
    public boolean isSuccess() {
        return m_success;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeBoolean(m_success);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_success = p_importer.readBoolean(m_success);
    }

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofBoolean();
    }
}
