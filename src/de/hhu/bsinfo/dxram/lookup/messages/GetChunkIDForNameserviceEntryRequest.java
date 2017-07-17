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
import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.net.core.AbstractMessageImporter;
import de.hhu.bsinfo.net.core.AbstractRequest;

/**
 * Request for getting the ChunkID to corresponding id on a remote node
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 */
public class GetChunkIDForNameserviceEntryRequest extends AbstractRequest {

    // Attributes
    private int m_id;

    // Constructors

    /**
     * Creates an instance of GetChunkIDRequest
     */
    public GetChunkIDForNameserviceEntryRequest() {
        super();

        m_id = -1;
    }

    /**
     * Creates an instance of GetChunkIDRequest
     *
     * @param p_destination
     *         the destination
     * @param p_id
     *         the id
     */
    public GetChunkIDForNameserviceEntryRequest(final short p_destination, final int p_id) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_GET_CHUNKID_FOR_NAMESERVICE_ENTRY_REQUEST);

        m_id = p_id;
    }

    // Getters

    /**
     * Get the id to store
     *
     * @return the id to store
     */
    public final int getID() {
        return m_id;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_id);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_id = p_importer.readInt(m_id);
    }

}
