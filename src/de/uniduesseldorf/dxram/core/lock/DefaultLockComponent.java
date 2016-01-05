package de.uniduesseldorf.dxram.core.lock;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import de.uniduesseldorf.utils.config.Configuration;
import de.uniduesseldorf.utils.locks.JNIReadWriteSpinLock;
import de.uniduesseldorf.utils.locks.SpinLock;

public class DefaultLockComponent extends LockComponent
{
	// Attributes
	private Map<Long, ReadWriteLock> m_locks;

	private Lock m_creationLock;
	
	public DefaultLockComponent(int p_priorityInit, int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

	@Override
	public void lock(long p_chunkID, boolean p_writeLock) {
		ReadWriteLock lock = getLock(p_chunkID);
		
		if (p_writeLock) {
			lock.writeLock().lock();
		} else {
			lock.readLock().lock();
		}
	}

	@Override
	public void unlock(long p_chunkID, boolean p_writeLock) {
		ReadWriteLock lock = getLock(p_chunkID);
		
		if (p_writeLock) {
			lock.writeLock().unlock();
		} else {
			lock.readLock().unlock();
		}
	}

	@Override
	protected void registerConfigurationValuesComponent(Configuration p_configuration) {

	}

	@Override
	protected boolean initComponent(Configuration p_configuration) {
		m_locks = new HashMap<Long, ReadWriteLock>();
		m_creationLock = new SpinLock();
		
		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		m_locks.clear();
		m_locks = null;
		m_creationLock = null;

		return true;
	}
	
	private ReadWriteLock getLock(final long p_chunkID) {
		ReadWriteLock lock = null;
		
		// try lock-less
		lock = m_locks.get(p_chunkID);
		if (lock == null) {
			m_creationLock.lock();
			
			// maybe someone added the lock right before we entered
			lock = m_locks.get(p_chunkID);
			if (lock == null) {
				// create lock and add
				lock = new JNIReadWriteSpinLock();
				m_locks.put(p_chunkID, lock);
			}
			
			m_creationLock.unlock();
		}
		
		return lock;
	}
}
