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
import de.hhu.bsinfo.net.core.AbstractMessage;
import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.net.core.AbstractMessageImporter;

/**
 * Message to free an allocation item on the superpeer storage.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.2015
 */
public class SuperpeerStorageRemoveMessage extends AbstractMessage {
    private int m_storageId;
    private boolean m_replicate;

    /**
     * Creates an instance of SuperpeerStorageRemoveMessage
     */
    public SuperpeerStorageRemoveMessage() {
        super();
    }

    /**
     * Creates an instance of SuperpeerStorageRemoveMessage
     *
     * @param p_destination
     *         the destination
     * @param p_storageId
     *         Storage id of an allocated block of memory on the superpeer.
     * @param p_replicate
     *         True if replicate message, false if not
     */
    public SuperpeerStorageRemoveMessage(final short p_destination, final int p_storageId, final boolean p_replicate) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_REMOVE_MESSAGE);

        m_storageId = p_storageId;
        m_replicate = p_replicate;
    }

    /**
     * Get the storage id to free on the current superpeer.
     *
     * @return Storage id to free.
     */
    public int getStorageId() {
        return m_storageId;
    }

    /**
     * Check if this request is a replicate message.
     *
     * @return True if replicate message.
     */
    public boolean isReplicate() {
        return m_replicate;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + Byte.BYTES;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_storageId);
        p_exporter.writeBoolean(m_replicate);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_storageId = p_importer.readInt();
        m_replicate = p_importer.readBoolean();
    }
}
