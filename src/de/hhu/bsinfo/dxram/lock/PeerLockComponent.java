package de.hhu.bsinfo.dxram.lock;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.util.NodeID;

/**
 * Implementation of the lock component interface. This provides a peer side locking i.e.
 * the peer owning the chunk stores any information about its locking state.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class PeerLockComponent extends LockComponent {

	private Map<Long, LockEntry> m_lockedChunks = null;
	private AtomicBoolean m_mapEntryCreationLock = null;
	
	private LoggerComponent m_logger = null;
	
	/**
	 * Constructor
	 * @param p_priorityInit Priority for initialization of this component. 
	 * 			When choosing the order, consider component dependencies here.
	 * @param p_priorityShutdown Priority for shutting down this component. 
	 * 			When choosing the order, consider component dependencies here.
	 */
	public PeerLockComponent(int p_priorityInit, int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

	@Override
	protected void registerDefaultSettingsComponent(Settings p_settings) {
	}

	@Override
	protected boolean initComponent(de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			Settings p_settings) {
		m_logger = getDependentComponent(LoggerComponent.class);
		
		m_lockedChunks = new ConcurrentHashMap<Long, LockEntry>();
		m_mapEntryCreationLock = new AtomicBoolean(false);
		
		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		m_logger = null;
		
		m_lockedChunks.clear();
		m_lockedChunks = null;
		m_mapEntryCreationLock = null;

		return true;
	}

	@Override
	public boolean lock(final long p_chunkId, final short p_lockingNodeID, boolean p_writeLock, int p_timeoutMs) 
	{
		boolean success = false;
		
		// sanity masking of localID
		LockEntry lockEntry = m_lockedChunks.get(ChunkID.getLocalID(p_chunkId));
		if (lockEntry == null) {
			// create on demand
			
			while (!m_mapEntryCreationLock.compareAndSet(false, true))
			;
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
			while (!lockEntry.m_lock.compareAndSet(false, true))
			;
			lockEntry.m_nodeID = p_lockingNodeID;
			success = true;
		} else {
			long tryLockTime = System.currentTimeMillis();
			while (true)
			{
				if (!lockEntry.m_lock.compareAndSet(false, true))
				{
					if (System.currentTimeMillis() - tryLockTime > p_timeoutMs) {
						break;
					}
				}
				else
				{
					lockEntry.m_nodeID = p_lockingNodeID;
					success = true;
					break;
				}
			}			
		}
		
		
		return success;
	}

	@Override
	public boolean unlock(final long p_chunkId, final short p_unlockingNodeID, boolean p_writeLock) {
		
		// sanity masking of localID
		LockEntry lockEntry = m_lockedChunks.get(ChunkID.getLocalID(p_chunkId));
		if (lockEntry == null) {
			// trying to unlock non locked chunk
			m_logger.error(getClass(), "Unlocking previously non locked chunk " + Long.toHexString(p_chunkId) + 
										" by node " + Integer.toHexString(p_unlockingNodeID & 0xFFFF) + " not possible.");
			return false;
		}
		
		if (lockEntry.m_nodeID != p_unlockingNodeID) {
			// trying to unlock a chunk we have not locked
			m_logger.error(getClass(), "Unlocking chunk " + Long.toHexString(p_chunkId) + 
									" locked by node " + Integer.toHexString(lockEntry.m_nodeID & 0xFFFF) + 
									" not allowed for node " + Integer.toHexString(p_unlockingNodeID & 0xFFFF) + ".");
			return false;
		}
		
		// TODO locks are not cleaned up after usage and it's not possible to
		// do this without further locking of the map involved (concurrent get not possible anymore)
		lockEntry.m_nodeID = NodeID.INVALID_ID;
		lockEntry.m_lock.set(false);
		return true;
	}
	
	@Override
	public boolean unlockAllByNodeID(final short p_nodeID)
	{
		// because the node crashed, we can assume that no further locks by this node are added
		for (Entry<Long, LockEntry> entry : m_lockedChunks.entrySet())
		{
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

	/**
	 * Entry for the lock map.
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
	 */
	private static class LockEntry
	{
		/**
		 * Lock for the chunk.
		 */
		public AtomicBoolean m_lock = new AtomicBoolean(false);
		
		/**
		 * ID of the node that has locked the chunk.
		 */
		public short m_nodeID = NodeID.INVALID_ID;
	}
}
