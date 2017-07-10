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
import de.hhu.bsinfo.net.core.AbstractResponse;

/**
 * Response to a InsertIDRequest
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 */
public class InsertNameserviceEntriesResponse extends AbstractResponse {

    // Attributes
    private short[] m_backupSuperpeers;

    // Constructors

    /**
     * Creates an instance of InsertIDResponse
     */
    public InsertNameserviceEntriesResponse() {
        super();

        m_backupSuperpeers = null;
    }

    /**
     * Creates an instance of InsertIDResponse
     *
     * @param p_request
     *         the request
     * @param p_backupSuperpeers
     *         the backup superpeers
     */
    public InsertNameserviceEntriesResponse(final InsertNameserviceEntriesRequest p_request, final short[] p_backupSuperpeers) {
        super(p_request, LookupMessages.SUBTYPE_INSERT_NAMESERVICE_ENTRIES_RESPONSE);

        m_backupSuperpeers = p_backupSuperpeers;
    }

    // Getters

    /**
     * Get the backup superpeers
     *
     * @return the backup superpeers
     */
    public final short[] getBackupSuperpeers() {
        return m_backupSuperpeers;
    }

    @Override
    protected final int getPayloadLength() {
        int ret;

        if (m_backupSuperpeers == null) {
            ret = Integer.BYTES;
        } else {
            ret = Integer.BYTES + m_backupSuperpeers.length * Short.BYTES;
        }

        return ret;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        if (m_backupSuperpeers == null) {
            p_exporter.writeInt(0);
        } else {
            p_exporter.writeShortArray(m_backupSuperpeers);
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_backupSuperpeers = p_importer.readShortArray();
    }

}
