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

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;

/**
 * Request to get data from the superpeer storage.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.2015
 */
public class SuperpeerStorageGetRequest extends Request {
    // the data structure is stored for the sender of the request
    // to write the incoming data of the response to it
    // the requesting IDs are taken from the structures
    private DataStructure m_dataStructure;
    // this is only used when receiving the request
    private int m_storageID;

    /**
     * Creates an instance of SuperpeerStorageGetRequest.
     * This constructor is used when receiving this message.
     */
    public SuperpeerStorageGetRequest() {
        super();
    }

    /**
     * Creates an instance of SuperpeerStorageGetRequest.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *         the destination node id.
     * @param p_dataStructure
     *         Data structure with the ID of the chunk to get.
     */
    public SuperpeerStorageGetRequest(final short p_destination, final DataStructure p_dataStructure) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_GET_REQUEST);

        m_dataStructure = p_dataStructure;
    }

    /**
     * Get the storage id.
     *
     * @return Storage id.
     */
    public int getStorageID() {
        return m_storageID;
    }

    /**
     * Get the data structures stored with this request.
     * This is used to write the received data to the provided object to avoid
     * using multiple buffers.
     *
     * @return Data structures to store data to when the response arrived.
     */
    public DataStructure getDataStructure() {
        return m_dataStructure;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeInt((int) m_dataStructure.getID());
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_storageID = p_importer.readInt(m_storageID);
    }
}
