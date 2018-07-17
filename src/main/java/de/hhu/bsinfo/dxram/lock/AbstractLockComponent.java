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

package de.hhu.bsinfo.dxram.lock;

import java.util.ArrayList;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponentConfig;

/**
 * Interface for a lock component providing locking of chunks.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public abstract class AbstractLockComponent<T extends AbstractDXRAMComponentConfig> extends AbstractDXRAMComponent<T> {
    static final int MS_TIMEOUT_UNLIMITED = -1;

    /**
     * Constructor
     *
     * @param p_priorityInit
     *         Default init priority for this component
     * @param p_priorityShutdown
     *         Default shutdown priority for this component
     * @param p_configClass
     *         Configuration class for this component
     */
    protected AbstractLockComponent(final short p_priorityInit, final short p_priorityShutdown, final Class<T> p_configClass) {
        super(p_priorityInit, p_priorityShutdown, p_configClass);
    }

    /**
     * Get a list of all currently locked chunks.
     *
     * @return List of currently locked chunks with nodes that locked them.
     */
    public abstract ArrayList<LockedChunkEntry> getLockedList();

    /**
     * Lock a chunk with the specified id (nodeID + localID).
     *
     * @param p_chunkId
     *         ChunkID of the chunk to lock.
     * @param p_lockingNodeID
     *         ID of the node that wants to lock
     * @param p_writeLock
     *         True to acquire a write lock, false for a read lock.
     * @param p_timeoutMs
     *         Timeout in ms for the lock operation. -1 for unlimited.
     * @return True if locking was successful, false for timeout.
     */
    public abstract boolean lock(long p_chunkId, short p_lockingNodeID, boolean p_writeLock, int p_timeoutMs);

    /**
     * Unlock a chunk with the specified ID (nodeID + localID).
     *
     * @param p_chunkId
     *         ChunkID of the chunk to lock
     * @param p_unlockingNodeID
     *         ID of the node that wants to unlock the chunk
     * @param p_writeLock
     *         True to unlock a write lock, false for a read lock.
     * @return True if unlocking was successful, false otherwise.
     */
    public abstract boolean unlock(long p_chunkId, short p_unlockingNodeID, boolean p_writeLock);

    /**
     * Unlock all chunks locked by a specific node ID.
     *
     * @param p_nodeID
     *         Node ID of the chunks to unlock.
     * @return True if unlocking chunks was successful, false otherwise.
     * @note This is only used in special scenarios (i.e. if a node has crashed).
     */
    public abstract boolean unlockAllByNodeID(short p_nodeID);
}
