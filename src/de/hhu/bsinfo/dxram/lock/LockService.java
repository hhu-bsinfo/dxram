package de.hhu.bsinfo.dxram.lock;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.DXRAMService;

public abstract class LockService extends DXRAMService {

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
	
	// timeout -1 for unlimited
	public ErrorCode lock(final boolean p_writeLock, final int p_timeout, final DataStructure p_dataStructure) {
		return lock(p_writeLock, p_timeout, p_dataStructure.getID());
	}
	
	public ErrorCode unlock(final boolean p_writeLock, final DataStructure p_dataStructure) {
		return unlock(p_writeLock, p_dataStructure.getID());
	}
	
	public abstract ErrorCode lock(final boolean p_writeLock, final int p_timeout, final long p_chunkID);
	
	public abstract ErrorCode unlock(final boolean p_writeLock, final long p_chunkID);
}
