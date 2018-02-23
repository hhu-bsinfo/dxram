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

package de.hhu.bsinfo.dxram.lookup.events;

import de.hhu.bsinfo.dxram.event.AbstractEvent;

/**
 * This event is fired when an existing nameservice entry is updated.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 19.04.2016
 */
public class NameserviceCacheEntryUpdateEvent extends AbstractEvent {

    private int m_id;
    private long m_chunkID;

    /**
     * Constructor
     *
     * @param p_sourceClass
     *     The calling class
     * @param p_id
     *     Id for the mapping.
     * @param p_chunkId
     *     Chunk id mapped to the id
     */
    public NameserviceCacheEntryUpdateEvent(final String p_sourceClass, final int p_id, final long p_chunkId) {
        super(p_sourceClass);

        m_id = p_id;
        m_chunkID = p_chunkId;
    }

    /**
     * Get the id for the mapping.
     *
     * @return Id.
     */
    public int getId() {
        return m_id;
    }

    /**
     * Get the chunk id mapped to the id.
     *
     * @return Chunk id.
     */
    public long getChunkID() {
        return m_chunkID;
    }
}
