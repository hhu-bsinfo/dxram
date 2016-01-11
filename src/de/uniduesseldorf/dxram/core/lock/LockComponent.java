package de.uniduesseldorf.dxram.core.lock;

import de.uniduesseldorf.dxram.core.engine.DXRAMComponent;

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
	public abstract boolean lock(final long p_chunkID, final boolean p_writeLock, final int p_timeoutMs);
	
	/**
	 * Unlock a chunk with the specified ID (nodeID + localID).
	 * @param p_chunkID ID of the chunk to unlock.
	 * @param p_writeLock True to unlock a write lock, false for a read lock.
	 */
	public abstract void unlock(final long p_chunkID, final boolean p_writeLock);
	
//	/**
//	 * Unlock all locally locked chunks.
//	 * @param p_writeLock True to unlock all write locks, false for a read locks.
//	 */
//	public abstract void unlockAll(final boolean p_writeLock);
}
