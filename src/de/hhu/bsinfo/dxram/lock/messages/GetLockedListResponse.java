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

package de.hhu.bsinfo.dxram.lock.messages;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.dxram.lock.LockedChunkEntry;
import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to a LockedListRequest
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 19.04.2016
 */
public class GetLockedListResponse extends AbstractResponse {

    private ArrayList<LockedChunkEntry> m_list;

    /**
     * Creates an instance of LockResponse as a receiver.
     */
    public GetLockedListResponse() {
        super();
    }

    /**
     * Creates an instance of LockResponse as a sender.
     *
     * @param p_request
     *     Corresponding request to this response.
     * @param p_lockedList
     *     List of locked chunks to send
     */
    public GetLockedListResponse(final GetLockedListRequest p_request, final ArrayList<LockedChunkEntry> p_lockedList) {
        super(p_request, LockMessages.SUBTYPE_GET_LOCKED_LIST_RESPONSE);

        m_list = p_lockedList;
    }

    /**
     * Get the list of locked chunks.
     *
     * @return List of locked chunks.
     */
    public ArrayList<LockedChunkEntry> getList() {
        return m_list;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + m_list.size() * LockedChunkEntry.SIZEOF_OBJECT;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        MessagesDataStructureImExporter imExporter = new MessagesDataStructureImExporter(p_buffer);

        p_buffer.putInt(m_list.size());
        for (LockedChunkEntry entry : m_list) {
            imExporter.exportObject(entry);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        MessagesDataStructureImExporter imExporter = new MessagesDataStructureImExporter(p_buffer);

        int size = p_buffer.getInt();
        m_list = new ArrayList<LockedChunkEntry>(size);

        for (int i = 0; i < size; i++) {
            LockedChunkEntry entry = new LockedChunkEntry();
            imExporter.importObject(entry);
            m_list.add(entry);
        }
    }
}
