/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;

/**
 * Response to the get request.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.2015
 */
public class SuperpeerStorageGetResponse extends Response {
    // The chunk objects here are used when sending the response only
    // when the response is received, the data structures from the request are
    // used to directly write the data to them and avoiding further copying
    private DataStructure m_dataStructure;
    private byte m_status;

    /**
     * Creates an instance of SuperpeerStorageGetResponse.
     * This constructor is used when receiving this message.
     */
    public SuperpeerStorageGetResponse() {
        super();
    }

    /**
     * Creates an instance of GetResponse.
     * This constructor is used when sending this message.
     * Make sure to include all the chunks with IDs from the request in the correct order. If a chunk does
     * not exist, no data and a length of 0 indicates this situation.
     *
     * @param p_request
     *         the corresponding GetRequest
     * @param p_dataStructure
     *         Data structure filled with the read data from memory
     */
    public SuperpeerStorageGetResponse(final SuperpeerStorageGetRequest p_request, final DataStructure p_dataStructure, final byte p_status) {
        super(p_request, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_GET_RESPONSE);
        m_dataStructure = p_dataStructure;
        m_status = p_status;
    }

    /**
     * Get the status
     *
     * @return the status
     */
    public int getStatus() {
        return m_status;
    }

    @Override
    protected final int getPayloadLength() {
        return m_dataStructure.sizeofObject() + Byte.BYTES;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        // read the data to be sent to the remote from the chunk set for this message
        p_exporter.exportObject(m_dataStructure);
        p_exporter.writeByte(m_status);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        // read the payload from the buffer and write it directly into
        // the data structure provided by the request to avoid further copying of data
        SuperpeerStorageGetRequest request = (SuperpeerStorageGetRequest) getCorrespondingRequest();

        m_dataStructure = request.getDataStructure();
        p_importer.importObject(m_dataStructure);
        m_status = p_importer.readByte(m_status);
    }
}
