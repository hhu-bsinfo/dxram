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
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.utils.NodeID;

/**
 * Implementation of the lock component interface. This provides a peer side locking i.e.
 * the peer owning the chunk stores any information about its locking state.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public class PeerLockComponent extends AbstractLockComponent<PeerLockComponentConfig> {

    private static final Logger LOGGER = LogManager.getFormatterLogger(PeerLockComponent.class.getSimpleName());

    private Map<Long, LockEntry> m_lockedChunks;
    private AtomicBoolean m_mapEntryCreationLock;

    /**
     * Constructor
     */
    public PeerLockComponent() {
        super(DXRAMComponentOrder.Init.PEER_LOCK, DXRAMComponentOrder.Shutdown.PEER_LOCK, PeerLockComponentConfig.class);
    }

    @Override
    public ArrayList<LockedChunkEntry> getLockedList() {
        ArrayList<LockedChunkEntry> ret = new ArrayList<>();
        for (Entry<Long, LockEntry> entry : m_lockedChunks.entrySet()) {

            LockEntry lockEntry = entry.getValue();
            short node = lockEntry.m_nodeID;
            if (node != NodeID.INVALID_ID) {
                ret.add(new LockedChunkEntry(entry.getKey(), node));
            }
        }

        return ret;
    }

    @Override
    public boolean lock(final long p_chunkId, final short p_lockingNodeID, final boolean p_writeLock, final int p_timeoutMs) {
        boolean success = false;

        // sanity masking of localID
        LockEntry lockEntry = m_lockedChunks.get(ChunkID.getLocalID(p_chunkId));
        if (lockEntry == null) {
            // create on demand

            while (!m_mapEntryCreationLock.compareAndSet(false, true)) {
                // Wait
            }

            LockEntry prev = m_lockedChunks.get(ChunkID.getLocalID(p_chunkId));
            // avoid race condition and use recently created lock if there is one
            if (prev != null) {
                lockEntry = prev;
            } else {
                lockEntry = new LockEntry();
                m_lockedChunks.put(ChunkID.getLocalID(p_chunkId), lockEntry);
            }
            m_mapEntryCreationLock.set(false);
        }

        if (p_timeoutMs == MS_TIMEOUT_UNLIMITED) {
            // unlimited timeout, lock
            while (!lockEntry.m_lock.compareAndSet(false, true)) {
                // Wait
            }
            lockEntry.m_nodeID = p_lockingNodeID;
            success = true;
        } else {
            long tryLockTime = System.currentTimeMillis();
            while (true) {
                if (!lockEntry.m_lock.compareAndSet(false, true)) {
                    if (System.currentTimeMillis() - tryLockTime > p_timeoutMs) {
                        break;
                    }
                } else {
                    lockEntry.m_nodeID = p_lockingNodeID;
                    success = true;
                    break;
                }
            }
        }

        return success;
    }

    @Override
    public boolean unlock(final long p_chunkId, final short p_unlockingNodeID, final boolean p_writeLock) {

        // sanity masking of localID
        LockEntry lockEntry = m_lockedChunks.get(ChunkID.getLocalID(p_chunkId));
        if (lockEntry == null) {
            // trying to unlock non locked chunk
            // #if LOGGER >= ERROR
            LOGGER.error("Unlocking previously non locked chunk 0x%X by node 0x%X not possible", p_chunkId, p_unlockingNodeID);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        if (lockEntry.m_nodeID != p_unlockingNodeID) {
            // trying to unlock a chunk we have not locked
            // #if LOGGER >= ERROR
            LOGGER.error("Unlocking chunk 0x%X locked by node 0x%X not allowed for node 0x%X", p_chunkId, lockEntry.m_nodeID, p_unlockingNodeID);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        // TODO locks are not cleaned up after usage and it's not possible to
        // do this without further locking of the map involved (concurrent get not possible anymore)
        lockEntry.m_nodeID = NodeID.INVALID_ID;
        lockEntry.m_lock.set(false);
        return true;
    }

    @Override
    public boolean unlockAllByNodeID(final short p_nodeID) {
        // because the node crashed, we can assume that no further locks by this node are added
        for (Entry<Long, LockEntry> entry : m_lockedChunks.entrySet()) {
            LockEntry lockEntry = entry.getValue();
            if (lockEntry.m_nodeID == p_nodeID) {
                // force unlock
                // TODO lock cleanup? refer to unlock function
                lockEntry.m_nodeID = NodeID.INVALID_ID;
                lockEntry.m_lock.set(false);
            }
        }

        return true;
    }

    @Override
    protected boolean supportsSuperpeer() {
        return false;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        // no dependencies
    }

    @Override
    protected boolean initComponent(final DXRAMContext.Config p_config) {
        m_lockedChunks = new ConcurrentHashMap<>();
        m_mapEntryCreationLock = new AtomicBoolean(false);

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        m_lockedChunks.clear();
        m_lockedChunks = null;
        m_mapEntryCreationLock = null;

        return true;
    }

    /**
     * Entry for the lock map.
     *
     * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
     */
    private static class LockEntry {
        /**
         * Lock for the chunk.
         */
        private AtomicBoolean m_lock = new AtomicBoolean(false);

        /**
         * ID of the node that has locked the chunk.
         */
        private short m_nodeID = NodeID.INVALID_ID;
    }
}
