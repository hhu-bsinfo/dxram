package de.hhu.bsinfo.dxram.lock;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;

/**
 * Service to lock chunks.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public abstract class LockService extends AbstractDXRAMService {

	public static final int MS_TIMEOUT_UNLIMITED = -1;
	
	public enum ErrorCode
	{
		SUCCESS,
		UNKNOWN,
		INVALID_PEER_ROLE,
		INVALID_PARAMETER,
		LOCK_TIMEOUT,
		CHUNK_NOT_AVAILABLE,
		PEER_NOT_AVAILABLE,
		NETWORK
	}
	
	/**
	 * Lock a DataStructure.
	 * @param p_writeLock True to acquire a write lock, false for read lock (implementation dependent).
	 * @param p_timeout -1 for unlimited (not recommended) or time in ms.
	 * @param p_dataStructure DataStructure to lock.
	 * @return ErrorCode of the operation (refer to enum).
	 */
	public ErrorCode lock(final boolean p_writeLock, final int p_timeout, final DataStructure p_dataStructure) {
		return lock(p_writeLock, p_timeout, p_dataStructure.getID());
	}
	
	/**
	 * Unlock a previously locked DataStructure.
	 * @param p_writeLock True to unlock a write lock, false for a read lock.
	 * @param p_dataStructure DataStructure to unlock.
	 * @return ErrorCode of the operation (refer to enum).
	 */
	public ErrorCode unlock(final boolean p_writeLock, final DataStructure p_dataStructure) {
		return unlock(p_writeLock, p_dataStructure.getID());
	}
	
	/**
	 * Lock a DataStructure.
	 * @param p_writeLock True to acquire a write lock, false for read lock (implementation dependent).
	 * @param p_timeout -1 for unlimited (not recommended) or time in ms.
	 * @param p_chunkID Chunk ID of the chunk to lock.
	 * @return ErrorCode of the operation (refer to enum).
	 */
	public abstract ErrorCode lock(final boolean p_writeLock, final int p_timeout, final long p_chunkID);
	
	/**
	 * Unlock a previously locked DataStructure.
	 * @param p_writeLock True to unlock a write lock, false for a read lock.
	 * @param p_chunkID Chunk ID to unlock.
	 * @return ErrorCode of the operation (refer to enum).
	 */
	public abstract ErrorCode unlock(final boolean p_writeLock, final long p_chunkID);
}
