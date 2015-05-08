
package de.uniduesseldorf.dxram.utils.locks;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import sun.misc.Unsafe;

import de.uniduesseldorf.dxram.utils.unsafe.UnsafeHandler;

/**
 * Represents a spinlock
 * @author Florian Klein 05.04.2014
 */
@SuppressWarnings("restriction")
public final class JNISpinLock implements Lock {

	// Constants
	private static final Unsafe UNSAFE = UnsafeHandler.getInstance().getUnsafe();

	// Attributes
	private long m_lock;

	// Constructors
	/**
	 * Creates an instance of SpinLock
	 */
	public JNISpinLock() {
		m_lock = UNSAFE.allocateMemory(4);
		UNSAFE.putInt(m_lock, 0);
	}

	// Methods
	@Override
	public void lock() {
		JNILock.lock(m_lock);
	}

	@Override
	public void unlock() {
		JNILock.unlock(m_lock);
	}

	@Override
	public void lockInterruptibly() throws InterruptedException {
		throw new UnsupportedOperationException("The method 'lockInterruptibly' is not supported.");
	}

	@Override
	public boolean tryLock() {
		return JNILock.tryLock(m_lock);
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

	@Override
	protected void finalize() {
		UNSAFE.freeMemory(m_lock);
	}

}
