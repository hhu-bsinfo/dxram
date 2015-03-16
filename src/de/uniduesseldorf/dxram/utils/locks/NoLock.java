
package de.uniduesseldorf.dxram.utils.locks;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Dummy lock (no real lock)
 * @author Florian Klein
 *         05.04.2014
 */
public final class NoLock implements Lock {

	// Constructors
	/**
	 * Creates an instance of NoLock
	 */
	public NoLock() {}

	// Methods
	@Override
	public void lock() {}

	@Override
	public void unlock() {}

	@Override
	public void lockInterruptibly() throws InterruptedException {}

	@Override
	public boolean tryLock() {
		return true;
	}

	@Override
	public boolean tryLock(final long p_time, final TimeUnit p_unit) throws InterruptedException {
		return true;
	}

	@Override
	public Condition newCondition() {
		return null;
	}

}
