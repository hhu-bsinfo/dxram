package de.hhu.bsinfo.dxram.lock;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.DXRAMService;

public abstract class LockService extends DXRAMService {

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
	
	public abstract ErrorCode lock(final boolean p_writeLock, final int p_timeout, final DataStructure p_dataStructure);
	
	public abstract ErrorCode unlock(final boolean p_writeLock, final DataStructure p_dataStructure);
}
