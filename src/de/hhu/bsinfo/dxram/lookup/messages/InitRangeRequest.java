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

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.net.core.AbstractMessageImporter;
import de.hhu.bsinfo.net.core.AbstractRequest;

/**
 * Init Range Request
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 03.06.2013
 */
public class InitRangeRequest extends AbstractRequest {

    // Attributes
    private short m_rangeOwner;
    private BackupRange m_backupRange;
    private boolean m_isBackup;

    // Constructors

    /**
     * Creates an instance of InitRangeRequest
     */
    public InitRangeRequest() {
        super();

        m_rangeOwner = -1;
        m_backupRange = null;
        m_isBackup = false;
    }

    /**
     * Creates an instance of InitRangeRequest
     *
     * @param p_destination
     *         the destination
     * @param p_rangeOwner
     *         the peer that created the new backup range
     * @param p_backupRange
     *         the backup range to initialize
     * @param p_isBackup
     *         whether this is a backup message or not
     */
    public InitRangeRequest(final short p_destination, final short p_rangeOwner, final BackupRange p_backupRange, final boolean p_isBackup) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_INIT_RANGE_REQUEST);

        m_rangeOwner = p_rangeOwner;
        m_backupRange = p_backupRange;
        m_isBackup = p_isBackup;
    }

    // Getters

    /**
     * Get the range owner
     *
     * @return the backup range owner
     */
    public final short getBackupRangeOwner() {
        return m_rangeOwner;
    }

    /**
     * Get the backup range
     *
     * @return the backup range
     */
    public final BackupRange getBackupRange() {
        return m_backupRange;
    }

    /**
     * Returns whether this is a backup message or not
     *
     * @return whether this is a backup message or not
     */
    public final boolean isBackup() {
        return m_isBackup;
    }

    @Override
    protected final int getPayloadLength() {
        return Short.BYTES + m_backupRange.sizeofObject() + Byte.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeShort(m_rangeOwner);
        p_exporter.exportObject(m_backupRange);
        p_exporter.writeBoolean(m_isBackup);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_rangeOwner = p_importer.readShort();
        m_backupRange = new BackupRange();
        p_importer.importObject(m_backupRange);
        m_isBackup = p_importer.readBoolean();
    }

}
