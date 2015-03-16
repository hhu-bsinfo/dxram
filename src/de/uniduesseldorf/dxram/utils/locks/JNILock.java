package de.uniduesseldorf.dxram.utils.locks;

/**
 * Provides lock functions for virtual memory addresses using JNI
 * @author Florian Klein
 *         14.10.2014
 */
public final class JNILock {

	// Statics
	static {
		System.load("/home/beineke/workspace/dxram/new_trunk/src/de/uniduesseldorf/dxram/utils/locks/libJNILock.so");
		// System.load("/Users/klein/Workspaces/DXRAM/DXRAM/jni/libJNILock.dylib");
	}

	// Constructors
	/**
	 * Creates an instance of JNILock
	 */
	private JNILock() {}

	// Methods
	/**
	 * Locks the read lock
	 * @param p_address
	 *            the address of the lock
	 */
	public static native void readLock(final long p_address);

	/**
	 * Unlocks the read lock
	 * @param p_address
	 *            the address of the lock
	 */
	public static native void readUnlock(final long p_address);

	/**
	 * Tries to lock the read lock
	 * @param p_address
	 *            the address of the lock
	 * @return true if the lock could be get, false otherwise
	 */
	public static native boolean tryReadLock(final long p_address);

	/**
	 * Locks the write lock
	 * @param p_address
	 *            the address of the lock
	 */
	public static native void writeLock(final long p_address);

	/**
	 * Unlocks the write lock
	 * @param p_address
	 *            the address of the lock
	 */
	public static native void writeUnlock(final long p_address);

	/**
	 * Tries to lock the write lock
	 * @param p_address
	 *            the address of the lock
	 * @return true if the lock could be get, false otherwise
	 */
	public static native boolean tryWriteLock(final long p_address);

	/**
	 * Locks the lock
	 * @param p_address
	 *            the address of the lock
	 */
	public static native void lock(final long p_address);

	/**
	 * Unlocks the lock
	 * @param p_address
	 *            the address of the lock
	 */
	public static native void unlock(final long p_address);

	/**
	 * Tries to lock the lock
	 * @param p_address
	 *            the address of the lock
	 * @return true if the lock could be get, false otherwise
	 */
	public static native boolean tryLock(final long p_address);

}
