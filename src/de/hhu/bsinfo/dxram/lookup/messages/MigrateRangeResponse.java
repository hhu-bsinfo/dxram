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
 * Response to a MigrateRangeRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class MigrateRangeResponse extends Response {

    // Attributes
    private boolean m_success;

    // Constructors

    /**
     * Creates an instance of MigrateRangeResponse
     */
    public MigrateRangeResponse() {
        super();

        m_success = false;
    }

    /**
     * Creates an instance of MigrateRangeResponse
     *
     * @param p_request
     *         the corresponding MigrateRangeRequest
     * @param p_success
     *         whether the migration was successful or not
     */
    public MigrateRangeResponse(final MigrateRangeRequest p_request, final boolean p_success) {
        super(p_request, LookupMessages.SUBTYPE_MIGRATE_RANGE_RESPONSE);

        m_success = p_success;
    }

    // Getters

    /**
     * Get the status
     *
     * @return whether the migration was successful or not
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
