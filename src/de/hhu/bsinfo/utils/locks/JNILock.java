package de.hhu.bsinfo.utils.locks;

/**
 * Provides lock functions for virtual memory addresses using JNI
 *
 * @author Florian Klein, florian.klein@hhu.de, 14.10.2014
 */
public final class JNILock {

    // Constructors

    /**
     * Creates an instance of JNILock
     */
    private JNILock() {
    }

    // Statics

    /**
     * Provide the path to the native implementation.
     *
     * @param p_pathNativeLibrary
     *         Path to the library with the native implementation.
     */
    public static void load(final String p_pathNativeLibrary) {
        System.load(p_pathNativeLibrary);
    }

    // Methods

    /**
     * Locks the read lock
     *
     * @param p_address
     *         the address of the lock
     */
    public static native void readLock(long p_address);

    /**
     * Unlocks the read lock
     *
     * @param p_address
     *         the address of the lock
     */
    public static native void readUnlock(long p_address);

    /**
     * Tries to lock the read lock
     *
     * @param p_address
     *         the address of the lock
     * @return true if the lock could be get, false otherwise
     */
    public static native boolean tryReadLock(long p_address);

    /**
     * Locks the write lock
     *
     * @param p_address
     *         the address of the lock
     */
    public static native void writeLock(long p_address);

    /**
     * Unlocks the write lock
     *
     * @param p_address
     *         the address of the lock
     */
    public static native void writeUnlock(long p_address);

    /**
     * Tries to lock the write lock
     *
     * @param p_address
     *         the address of the lock
     * @return true if the lock could be get, false otherwise
     */
    public static native boolean tryWriteLock(long p_address);

    /**
     * Locks the lock
     *
     * @param p_address
     *         the address of the lock
     */
    public static native void lock(long p_address);

    /**
     * Unlocks the lock
     *
     * @param p_address
     *         the address of the lock
     */
    public static native void unlock(long p_address);

    /**
     * Tries to lock the lock
     *
     * @param p_address
     *         the address of the lock
     * @return true if the lock could be get, false otherwise
     */
    public static native boolean tryLock(long p_address);

}
