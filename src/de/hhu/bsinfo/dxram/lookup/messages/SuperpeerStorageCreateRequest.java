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
 * Message to allocate memory in the superpeer storage.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.2015
 */
public class SuperpeerStorageCreateRequest extends AbstractRequest {
    private int m_storageId;
    private int m_size;
    private boolean m_replicate;

    /**
     * Creates an instance of SuperpeerStorageCreateRequest
     */
    public SuperpeerStorageCreateRequest() {
        super();
    }

    /**
     * Creates an instance of SuperpeerStorageCreateRequest
     *
     * @param p_destination
     *         the destination
     * @param p_storageId
     *         Identifier for the chunk.
     * @param p_size
     *         Size in bytes of the data to store
     * @param p_replicate
     *         True if this message is a replication to other superpeer message, false if normal message
     */
    public SuperpeerStorageCreateRequest(final short p_destination, final int p_storageId, final int p_size, final boolean p_replicate) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_CREATE_REQUEST);

        m_storageId = p_storageId;
        m_size = p_size;
        m_replicate = p_replicate;
    }

    /**
     * Get the storage id to use for this memory block
     *
     * @return Storage id.
     */
    public int getStorageId() {
        return m_storageId;
    }

    /**
     * Get the size for the allocation
     *
     * @return Size in bytes
     */
    public int getSize() {
        return m_size;
    }

    /**
     * Check if this message is a replicate message.
     *
     * @return True if replicate message, false otherwise.
     */
    public boolean isReplicate() {
        return m_replicate;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + Integer.BYTES + Byte.BYTES;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_storageId);
        p_exporter.writeInt(m_size);
        p_exporter.writeBoolean(m_replicate);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_storageId = p_importer.readInt();
        m_size = p_importer.readInt();
        m_replicate = p_importer.readBoolean();
    }
}
