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

package de.hhu.bsinfo.dxram.migration.messages;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Request for storing a Chunk on a remote node after migration
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.12.2015
 */
public class MigrationRequest extends Request {

    // data structure is used when request is sent
    // chunks are created if data is received
    private DataStructure[] m_dataStructures;

    // used when receiving the request
    private long[] m_chunkIDs;
    private byte[][] m_data;

    /**
     * Creates an instance of DataRequest.
     * This constructor is used when receiving this message.
     */
    public MigrationRequest() {
        super();
    }

    /**
     * Creates an instance of DataRequest.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *         the destination
     * @param p_dataStructures
     *         The data structures to migrate.
     */
    public MigrationRequest(final short p_destination, final DataStructure... p_dataStructures) {
        super(p_destination, DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE, MigrationMessages.SUBTYPE_MIGRATION_REQUEST);
        m_dataStructures = p_dataStructures;
    }

    /**
     * Get the chunk IDs of the data to migrate when this message is received.
     *
     * @return the IDs of the chunks to migrate
     */
    public long[] getChunkIDs() {
        return m_chunkIDs;
    }

    /**
     * Get the data of the chunks to migrate when this message is received
     *
     * @return Array of byte[] of chunk data to migrate
     */
    public byte[][] getChunkData() {
        return m_data;
    }

    @Override
    protected final int getPayloadLength() {
        int size = 0;

        if (m_dataStructures != null) {
            size += ObjectSizeUtil.sizeofCompactedNumber(m_dataStructures.length);
            size += m_dataStructures.length * Long.BYTES;

            for (DataStructure dataStructure : m_dataStructures) {
                size += ObjectSizeUtil.sizeofCompactedNumber(dataStructure.sizeofObject());
                size += dataStructure.sizeofObject();
            }
        } else {
            size += ObjectSizeUtil.sizeofCompactedNumber(m_chunkIDs.length);
            size += m_chunkIDs.length * Long.BYTES;

            for (int i = 0; i < m_data.length; i++) {
                size += ObjectSizeUtil.sizeofCompactedNumber(m_data[i].length);
                size += m_data[i].length;
            }
        }

        return size;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeCompactNumber(m_dataStructures.length);
        for (DataStructure dataStructure : m_dataStructures) {
            p_exporter.writeLong(dataStructure.getID());
            p_exporter.writeCompactNumber(dataStructure.sizeofObject());
            p_exporter.exportObject(dataStructure);
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        int length = p_importer.readCompactNumber(0);
        if (m_chunkIDs == null) {
            // Do not overwrite existing arrays
            m_chunkIDs = new long[length];
            m_data = new byte[length][];
        }
        for (int i = 0; i < m_chunkIDs.length; i++) {
            m_chunkIDs[i] = p_importer.readLong(m_chunkIDs[i]);
            m_data[i] = p_importer.readByteArray(m_data[i]);
        }
    }

}
