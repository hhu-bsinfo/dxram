/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

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
