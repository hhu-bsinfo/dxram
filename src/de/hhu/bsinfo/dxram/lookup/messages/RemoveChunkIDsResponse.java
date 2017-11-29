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

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Response to a RemoveRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class RemoveChunkIDsResponse extends Response {

    // Attributes
    private short[] m_backupSuperpeers;

    // Constructors

    /**
     * Creates an instance of RemoveResponse
     */
    public RemoveChunkIDsResponse() {
        super();

        m_backupSuperpeers = null;
    }

    /**
     * Creates an instance of RemoveResponse
     *
     * @param p_request
     *         the corresponding RemoveRequest
     * @param p_backupSuperpeers
     *         the backup superpeers
     */
    public RemoveChunkIDsResponse(final RemoveChunkIDsRequest p_request, final short[] p_backupSuperpeers) {
        super(p_request, LookupMessages.SUBTYPE_REMOVE_CHUNKIDS_RESPONSE);

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
        if (m_backupSuperpeers != null) {
            return ObjectSizeUtil.sizeofShortArray(m_backupSuperpeers);
        } else {
            return Byte.BYTES;
        }
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        if (m_backupSuperpeers == null) {
            p_exporter.writeCompactNumber(0);
        } else {
            p_exporter.writeShortArray(m_backupSuperpeers);
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_backupSuperpeers = p_importer.readShortArray(m_backupSuperpeers);
    }

}
