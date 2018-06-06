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

import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Response to a GetBackupRangesRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 08.10.2015
 */
public class GetAllBackupRangesResponse extends Response {

    // Attributes
    private BackupRange[] m_backupRanges;

    // Constructors

    /**
     * Creates an instance of GetBackupRangesResponse
     */
    public GetAllBackupRangesResponse() {
        super();

        m_backupRanges = null;
    }

    /**
     * Creates an instance of GetBackupRangesResponse
     *
     * @param p_request
     *         the corresponding GetBackupRangesRequest
     * @param p_backupRanges
     *         all backup ranges for requested NodeID
     */
    public GetAllBackupRangesResponse(final GetAllBackupRangesRequest p_request, final BackupRange[] p_backupRanges) {
        super(p_request, LookupMessages.SUBTYPE_GET_ALL_BACKUP_RANGES_RESPONSE);

        m_backupRanges = p_backupRanges;
    }

    // Getters

    /**
     * Get all backup ranges
     *
     * @return all backup ranges
     */
    public final BackupRange[] getBackupRanges() {
        return m_backupRanges;
    }

    @Override
    protected final int getPayloadLength() {
        int ret = ObjectSizeUtil.sizeofCompactedNumber(m_backupRanges.length);

        for (BackupRange backupRange : m_backupRanges) {
            ret += backupRange.sizeofObject();
        }

        return ret;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        if (m_backupRanges == null) {
            p_exporter.writeCompactNumber(0);
        } else {
            p_exporter.writeCompactNumber(m_backupRanges.length);
            for (BackupRange backupRange : m_backupRanges) {
                p_exporter.exportObject(backupRange);
            }
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        int length = p_importer.readCompactNumber(0);
        if (m_backupRanges == null) {
            m_backupRanges = new BackupRange[length];
        }
        for (int i = 0; i < m_backupRanges.length; i++) {
            if (m_backupRanges[i] == null) {
                m_backupRanges[i] = new BackupRange();
            }
            p_importer.importObject(m_backupRanges[i]);
        }
    }

}
