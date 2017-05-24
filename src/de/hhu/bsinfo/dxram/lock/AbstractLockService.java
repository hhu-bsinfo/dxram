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

package de.hhu.bsinfo.dxram.lock;

import java.util.ArrayList;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;

/**
 * Service to lock chunks.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public abstract class AbstractLockService<T extends AbstractDXRAMServiceConfig> extends AbstractDXRAMService<T> {

    /**
     * Error codes for some methods.
     *
     * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
     */
    public enum ErrorCode {
        SUCCESS, UNKNOWN, INVALID_PEER_ROLE, INVALID_PARAMETER, LOCK_TIMEOUT, CHUNK_NOT_AVAILABLE, PEER_NOT_AVAILABLE, NETWORK
    }

    public static final int MS_TIMEOUT_UNLIMITED = -1;

    /**
     * Constructor
     *
     * @param p_configClass
     *         Configuration class for this service
     */
    AbstractLockService(final Class<T> p_configClass) {
        super("lock", p_configClass);
    }

    /**
     * Get a list of of locked chunk of the current node.
     *
     * @return List of currently locked chunks, null on error.
     */
    public abstract ArrayList<LockedChunkEntry> getLockedList();

    /**
     * Get a list of of locked chunk from a specific node.
     *
     * @param p_nodeId
     *         Id of the node to get the list from.
     * @return List of currently locked chunks, null on error.
     */
    public abstract ArrayList<LockedChunkEntry> getLockedList(short p_nodeId);

    /**
     * Lock a DataStructure.
     *
     * @param p_writeLock
     *         True to acquire a write lock, false for read lock (implementation dependent).
     * @param p_timeout
     *         -1 for unlimited (not recommended) or time in ms.
     * @param p_dataStructure
     *         DataStructure to lock.
     * @return ErrorCode of the operation (refer to enum).
     */
    public ErrorCode lock(final boolean p_writeLock, final int p_timeout, final DataStructure p_dataStructure) {
        return lock(p_writeLock, p_timeout, p_dataStructure.getID());
    }

    /**
     * Unlock a previously locked DataStructure.
     *
     * @param p_writeLock
     *         True to unlock a write lock, false for a read lock.
     * @param p_dataStructure
     *         DataStructure to unlock.
     * @return ErrorCode of the operation (refer to enum).
     */
    public ErrorCode unlock(final boolean p_writeLock, final DataStructure p_dataStructure) {
        return unlock(p_writeLock, p_dataStructure.getID());
    }

    /**
     * Lock a DataStructure.
     *
     * @param p_writeLock
     *         True to acquire a write lock, false for read lock (implementation dependent).
     * @param p_timeout
     *         -1 for unlimited (not recommended) or time in ms.
     * @param p_chunkID
     *         Chunk ID of the chunk to lock.
     * @return ErrorCode of the operation (refer to enum).
     */
    public abstract ErrorCode lock(boolean p_writeLock, int p_timeout, long p_chunkID);

    /**
     * Unlock a previously locked DataStructure.
     *
     * @param p_writeLock
     *         True to unlock a write lock, false for a read lock.
     * @param p_chunkID
     *         Chunk ID to unlock.
     * @return ErrorCode of the operation (refer to enum).
     */
    public abstract ErrorCode unlock(boolean p_writeLock, long p_chunkID);
}
