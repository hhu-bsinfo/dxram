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

package de.hhu.bsinfo.dxram.data;

import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Importable;

/**
 * Base class for any kind of data structure that can be stored to and read from
 * the key value store. Implement this with any object you want to put to/get from the system.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.12.2015
 */
public abstract class DataStructure implements Importable, Exportable {
    private long m_id = ChunkID.INVALID_ID;
    private ChunkState m_state = ChunkState.UNDEFINED;

    /**
     * Default constructor
     */
    public DataStructure() {

    }

    /**
     * Constructor
     *
     * @param p_chunkID
     *         Chunk ID to assign
     */
    public DataStructure(final long p_chunkID) {
        m_id = p_chunkID;
    }

    /**
     * Get the unique identifier of this data structure.
     *
     * @return Unique identifier.
     */
    public long getID() {
        return m_id;
    }

    /**
     * Set the unique identifier of this data structure.
     *
     * @param p_id
     *         ID to set.
     */
    public void setID(final long p_id) {
        m_id = p_id;
    }

    /**
     * Get the current state of the data structure. The state is set by DXRAM when execution DXRAM operations
     * on the data structure
     *
     * @return State set by the last DXRAM operation involving this data structure
     */
    public ChunkState getState() {
        return m_state;
    }

    /**
     * Set the state of the data structure. This is used by DXRAM on operations to indicate errors on operations.
     * Applications built on top of DXRAM do not need this call.
     *
     * @param p_state
     *         State to set.
     */
    public void setState(final ChunkState p_state) {
        m_state = p_state;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + '[' + ChunkID.toHexString(m_id) + ", " + m_state + ']';
    }
}
