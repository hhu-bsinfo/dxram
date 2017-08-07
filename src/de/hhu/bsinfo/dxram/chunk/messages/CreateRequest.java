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

package de.hhu.bsinfo.dxram.chunk.messages;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.utils.serialization.ObjectSizeUtil;

/**
 * Request to create new chunks remotely.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 13.01.2016
 */
public class CreateRequest extends Request {
    private int[] m_sizes;

    /**
     * Creates an instance of CreateRequest.
     * This constructor is used when receiving this message.
     */
    public CreateRequest() {
        super();
    }

    /**
     * Creates an instance of CreateRequest.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *         the destination node id.
     * @param p_sizes
     *         Sizes of the chunks to create.
     */
    public CreateRequest(final short p_destination, final int... p_sizes) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_CREATE_REQUEST);
        m_sizes = p_sizes;
    }

    /**
     * Get the sizes received.
     *
     * @return Array of sizes to create chunks of.
     */
    public int[] getSizes() {
        return m_sizes;
    }

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofIntArray(m_sizes);
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeIntArray(m_sizes);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_sizes = p_importer.readIntArray(m_sizes);
    }
}
