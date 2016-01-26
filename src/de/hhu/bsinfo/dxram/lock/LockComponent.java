package de.hhu.bsinfo.dxram.lock;

import de.hhu.bsinfo.dxram.engine.DXRAMComponent;

/**
 * Interface for a lock component providing locking of chunks.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public abstract class LockComponent extends DXRAMComponent {

	public static final int MS_TIMEOUT_UNLIMITED = -1;
	
	/**
	 * Constructor
	 * @param p_priorityInit Priority for initialization of this component. 
	 * 			When choosing the order, consider component dependencies here.
	 * @param p_priorityShutdown Priority for shutting down this component. 
	 * 			When choosing the order, consider component dependencies here.
	 */
	public LockComponent(int p_priorityInit, int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

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
	
	/**
	 * Unlock all chunks locked by a specific node ID.
	 * @note This is only used in special scenarios (i.e. if a node has crashed).
	 * @param p_nodeID Node ID of the chunks to unlock.
	 * @return True if unlocking chunks was successful, false otherwise.
	 */
	public abstract boolean unlockAllByNodeID(final short p_nodeID);
}
