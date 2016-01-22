package de.hhu.bsinfo.dxram.lock;

import de.hhu.bsinfo.dxram.engine.DXRAMComponent;

public abstract class LockComponent extends DXRAMComponent {

	public LockComponent(int p_priorityInit, int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

	// Methods
	/**
	 * Lock a chunk with the specified id (nodeID + localID).
	 * @param p_chunkID ID of the chunk to lock.
	 * @param p_writeLock True to acquire a write lock, false for a read lock.
	 * @param p_timeout Timeout in ms for the lock operation. -1 for unlimited.
	 * @return True if locking was successful, false for timeout.
	 */
	public abstract boolean lock(final long p_localID, final short p_lockingNodeID, final boolean p_writeLock, final int p_timeoutMs);
	
	/**
	 * Unlock a chunk with the specified ID (nodeID + localID).
	 * @param p_chunkID ID of the chunk to unlock.
	 * @param p_writeLock True to unlock a write lock, false for a read lock.
	 */
	public abstract boolean unlock(final long p_localID, final short p_unlockingNodeID, final boolean p_writeLock);
	
	// used when node crashed
	public abstract boolean unlockAllByNodeID(final short p_nodeID);
}
