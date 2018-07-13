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
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.RangeID;

/**
 * Request for initialization of a backup range on a remote node
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 20.04.2016
 */
public class InitBackupRangeRequest extends Request {

    // Attributes
    private short m_rangeID = RangeID.INVALID_ID;

    // Constructors

    /**
     * Creates an instance of InitBackupRangeRequest
     */
    public InitBackupRangeRequest() {
        super();
    }

    /**
     * Creates an instance of InitBackupRangeRequest
     *
     * @param p_destination
     *         the destination
     * @param p_rangeID
     *         the RangeID
     */
    public InitBackupRangeRequest(final short p_destination, final short p_rangeID) {
        super(p_destination, DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_INIT_BACKUP_RANGE_REQUEST, true);

        m_rangeID = p_rangeID;
    }

    // Getters

    /**
     * Get the RangeID
     *
     * @return the RangeID
     */
    public final short getRangeID() {
        return m_rangeID;
    }

    @Override
    protected final int getPayloadLength() {
        return Short.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeShort(m_rangeID);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_rangeID = p_importer.readShort(m_rangeID);
    }
}
