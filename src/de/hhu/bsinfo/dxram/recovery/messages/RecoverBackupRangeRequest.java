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

package de.hhu.bsinfo.dxram.recovery.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Recover Backup Range Message
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 08.10.2015
 */
public class RecoverBackupRangeRequest extends AbstractRequest {

    // Attributes
    private short m_owner;
    private BackupRange m_backupRange;

    // Constructors

    /**
     * Creates an instance of RecoverBackupRangeRequest
     */
    public RecoverBackupRangeRequest() {
        super();

        m_owner = NodeID.INVALID_ID;
        m_backupRange = null;
    }

    /**
     * Creates an instance of RecoverBackupRangeRequest
     *
     * @param p_destination
     *     the destination
     * @param p_owner
     *     the NodeID of the owner
     * @param p_backupRange
     *     the backup range to recover
     */
    public RecoverBackupRangeRequest(final short p_destination, final short p_owner, final BackupRange p_backupRange) {
        super(p_destination, DXRAMMessageTypes.RECOVERY_MESSAGES_TYPE, RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_REQUEST, true);

        m_owner = p_owner;
        m_backupRange = p_backupRange;
    }

    // Getters

    /**
     * Get the owner
     *
     * @return the NodeID
     */
    public final short getOwner() {
        return m_owner;
    }

    /**
     * Get the backup range
     *
     * @return the backup range
     */
    public final BackupRange getBackupRange() {
        return m_backupRange;
    }

    @Override
    protected final int getPayloadLength() {
        return Short.BYTES + m_backupRange.sizeofObject();
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);
        exporter.exportObject(m_backupRange);

        p_buffer.putShort(m_owner);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);
        m_backupRange = new BackupRange();
        importer.importObject(m_backupRange);

        m_owner = p_buffer.getShort();
    }

}
