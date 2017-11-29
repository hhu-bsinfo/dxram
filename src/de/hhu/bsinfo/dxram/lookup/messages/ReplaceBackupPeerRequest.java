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
import de.hhu.bsinfo.dxram.backup.RangeID;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Replace Backup Peer Request
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 21.10.2016
 */
public class ReplaceBackupPeerRequest extends Request {

    // Attributes
    private short m_rangeID;
    private short m_failedBackupPeer;
    private short m_newBackupPeer;
    private boolean m_isBackup;

    // Constructors

    /**
     * Creates an instance of ReplaceBackupPeerRequest
     */
    public ReplaceBackupPeerRequest() {
        super();

        m_rangeID = RangeID.INVALID_ID;
        m_failedBackupPeer = NodeID.INVALID_ID;
        m_newBackupPeer = NodeID.INVALID_ID;
        m_isBackup = false;
    }

    /**
     * Creates an instance of ReplaceBackupPeerRequest
     *
     * @param p_destination
     *         the destination
     * @param p_rangeID
     *         the RangeID
     * @param p_failedBackupPeer
     *         the failed backup peer
     * @param p_newBackupPeer
     *         the replacement
     * @param p_isBackup
     *         whether this is a backup or not
     */
    public ReplaceBackupPeerRequest(final short p_destination, final short p_rangeID, final short p_failedBackupPeer, final short p_newBackupPeer,
            final boolean p_isBackup) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_REPLACE_BACKUP_PEER_REQUEST);

        m_rangeID = p_rangeID;
        m_failedBackupPeer = p_failedBackupPeer;
        m_newBackupPeer = p_newBackupPeer;
        m_isBackup = p_isBackup;
    }

    // Getters

    /**
     * Get the RangeID
     *
     * @return the ID
     */
    public final short getRangeID() {
        return m_rangeID;
    }

    /**
     * Get the NodeID of the failed peer
     *
     * @return the NodeID
     */
    public final short getFailedPeer() {
        return m_failedBackupPeer;
    }

    /**
     * Get the NodeID of the new backup peer
     *
     * @return the NodeID
     */
    public final short getNewPeer() {
        return m_newBackupPeer;
    }

    /**
     * Return if is is a backup
     *
     * @return whether this is a backup or not
     */
    public final boolean isBackup() {
        return m_isBackup;
    }

    @Override
    protected final int getPayloadLength() {
        return 3 * Short.BYTES + Byte.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeShort(m_rangeID);
        p_exporter.writeShort(m_failedBackupPeer);
        p_exporter.writeShort(m_newBackupPeer);
        p_exporter.writeBoolean(m_isBackup);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_rangeID = p_importer.readShort(m_rangeID);
        m_failedBackupPeer = p_importer.readShort(m_failedBackupPeer);
        m_newBackupPeer = p_importer.readShort(m_newBackupPeer);
        m_isBackup = p_importer.readBoolean(m_isBackup);
    }

}
