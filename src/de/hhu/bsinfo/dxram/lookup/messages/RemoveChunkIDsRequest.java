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
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.utils.ArrayListLong;
import de.hhu.bsinfo.utils.serialization.ObjectSizeUtil;

/**
 * Remove Request
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class RemoveChunkIDsRequest extends Request {

    // Attributes
    private ArrayListLong m_chunkIDsOut;
    private long[] m_chunkIDs;
    private boolean m_isBackup;

    // Constructors

    /**
     * Creates an instance of RemoveRequest
     */
    public RemoveChunkIDsRequest() {
        super();

        m_chunkIDs = null;
        m_isBackup = false;
    }

    /**
     * Creates an instance of RemoveRequest
     *
     * @param p_destination
     *         the destination
     * @param p_chunkIDs
     *         the ChunkIDs that have to be removed
     * @param p_isBackup
     *         whether this is a backup message or not
     */
    public RemoveChunkIDsRequest(final short p_destination, final ArrayListLong p_chunkIDs, final boolean p_isBackup) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_REMOVE_CHUNKIDS_REQUEST);

        assert p_chunkIDs != null;

        m_chunkIDsOut = p_chunkIDs;
        m_isBackup = p_isBackup;
    }

    // Getters

    /**
     * Get the ChunkID
     *
     * @return the ChunkID
     */
    public final long[] getChunkIDs() {
        return m_chunkIDs;
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

        if (m_chunkIDsOut != null) {
            return m_chunkIDsOut.sizeofObject() + Byte.BYTES;
        } else {
            return ObjectSizeUtil.sizeofLongArray(m_chunkIDs) + Byte.BYTES;
        }
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.exportObject(m_chunkIDsOut);
        p_exporter.writeBoolean(m_isBackup);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_chunkIDs = p_importer.readLongArray(m_chunkIDs);
        m_isBackup = p_importer.readBoolean(m_isBackup);
    }

}
