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

package de.hhu.bsinfo.dxram.log.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;

/**
 * Response to a InitBackupRangeRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 20.04.2016
 */
public class InitBackupRangeResponse extends Response {

    // Attributes
    private boolean m_success;

    // Constructors

    /**
     * Creates an instance of InitBackupRangeResponse
     */
    public InitBackupRangeResponse() {
        super();

        m_success = false;
    }

    /**
     * Creates an instance of InitBackupRangeResponse
     *
     * @param p_request
     *         the request
     * @param p_success
     *         true if remove was successful
     */
    public InitBackupRangeResponse(final InitBackupRangeRequest p_request, final boolean p_success) {
        super(p_request, LogMessages.SUBTYPE_INIT_BACKUP_RANGE_RESPONSE);

        m_success = p_success;
    }

    // Getters

    /**
     * Get the status
     *
     * @return true if remove was successful
     */
    public final boolean getStatus() {
        return m_success;
    }

    @Override
    protected final int getPayloadLength() {
        return Byte.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeBoolean(m_success);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_success = p_importer.readBoolean(m_success);
    }

}
