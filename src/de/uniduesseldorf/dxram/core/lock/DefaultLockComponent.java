package de.uniduesseldorf.dxram.core.lock;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import de.uniduesseldorf.dxram.core.engine.DXRAMEngine;

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
	public boolean lock(long p_chunkID, boolean p_writeLock, final int p_timeoutMs) {
		ReadWriteLock lock = getLock(p_chunkID);
		boolean success = false;
		
		if (p_writeLock) {
			if (p_timeoutMs == -1) {
				success = true;
				lock.writeLock().lock();
			} else {
				try {
					success = lock.writeLock().tryLock(p_timeoutMs, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
				}
			}
		} else {
			if (p_timeoutMs == -1) {
				success = true;
				lock.readLock().lock();
			} else {
				try {
					success = lock.readLock().tryLock(p_timeoutMs, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
				}
			}
		}
		
		return success;
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
	protected void registerDefaultSettingsComponent(final Settings p_settings) {
	}

	@Override
	protected boolean initComponent(final DXRAMEngine.Settings p_engineSettings, final Settings p_settings) {
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
