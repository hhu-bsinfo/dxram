
package de.uniduesseldorf.dxram.utils.locks;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import sun.misc.Unsafe;

import de.uniduesseldorf.dxram.utils.unsafe.UnsafeHandler;

/**
 * Represents a spinlock
 * @author Florian Klein 05.04.2014
 */
public final class JNIReadWriteSpinLock implements ReadWriteLock {

	// Constants
	private static final Unsafe UNSAFE = UnsafeHandler.getInstance().getUnsafe();

	// Attributes
	private long m_lock;
	private ReadLock m_readLock;
	private WriteLock m_writeLock;

	// Constructors
	/**
	 * Creates an instance of ReadWriteSpinLock
	 */
	public JNIReadWriteSpinLock() {
		m_lock = UNSAFE.allocateMemory(4);
		UNSAFE.putInt(m_lock, 0);
		m_readLock = new ReadLock();
		m_writeLock = new WriteLock();
	}

	// Methods
	@Override
	public Lock readLock() {
		return m_readLock;
	}

	@Override
	public Lock writeLock() {
		return m_writeLock;
	}

	@Override
	protected void finalize() {
		UNSAFE.freeMemory(m_lock);
	}

	// Classes
	/**
	 * Represent the ReadLock part
	 * @author Florian Klein 27.10.2014
	 */
	private final class ReadLock implements Lock {

		// Constructors
		/**
		 * Creates an instance of ReadLock
		 */
		private ReadLock() {}

		// Methods
		@Override
		public void lock() {
			JNILock.readLock(m_lock);
		}

		@Override
		public void unlock() {
			JNILock.readUnlock(m_lock);
		}

		@Override
		public void lockInterruptibly() throws InterruptedException {
			throw new UnsupportedOperationException("The method 'lockInterruptibly' is not supported.");
		}

		@Override
		public boolean tryLock() {
			return JNILock.tryReadLock(m_lock);
		}

		@Override
		public boolean tryLock(final long p_time, final TimeUnit p_unit) throws InterruptedException {
			boolean ret;
			long time;
			long nanos;

			ret = tryLock();
			if (!ret) {
				nanos = p_unit.toNanos(p_time);
				time = System.nanoTime();

				while (!ret && System.nanoTime() < time + nanos) {
					ret = tryLock();
				}
			}

			return ret;
		}

		@Override
		public Condition newCondition() {
			throw new UnsupportedOperationException("The method 'newCondition' is not supported.");
		}

	}

	/**
	 * Represent the WriteLock part
	 * @author Florian Klein 27.10.2014
	 */
	private final class WriteLock implements Lock {

		// Constructors
		/**
		 * Creates an instance of WriteLock
		 */
		private WriteLock() {}

		// Methods
		@Override
		public void lock() {
			JNILock.writeLock(m_lock);
		}

		@Override
		public void unlock() {
			JNILock.writeUnlock(m_lock);
		}

		@Override
		public void lockInterruptibly() throws InterruptedException {
			throw new UnsupportedOperationException("The method 'lockInterruptibly' is not supported.");
		}

		@Override
		public boolean tryLock() {
			return JNILock.tryWriteLock(m_lock);
		}

		@Override
		public boolean tryLock(final long p_time, final TimeUnit p_unit) throws InterruptedException {
			boolean ret;
			long time;
			long nanos;

			ret = tryLock();
			if (!ret) {
				nanos = p_unit.toNanos(p_time);
				time = System.nanoTime();

				while (!ret && System.nanoTime() < time + nanos) {
					ret = tryLock();
				}
			}

			return ret;
		}

		@Override
		public Condition newCondition() {
			throw new UnsupportedOperationException("The method 'newCondition' is not supported.");
		}

	}

}
