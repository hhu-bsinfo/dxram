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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import sun.misc.Unsafe;

import de.hhu.bsinfo.utils.UnsafeHandler;

/**
 * Represents a spinlock
 *
 * @author Florian Klein, florian.klein@hhu.de, 05.04.2014
 */
public final class SpinLock implements Lock {

    // Constants
    private static final Unsafe UNSAFE = UnsafeHandler.getInstance().getUnsafe();
    private static final long OFFSET = getLockOffset();

    // Attributes
    @SuppressWarnings("unused") private volatile int m_lock;

    // Constructors

    /**
     * Creates an instance of SpinLock
     */
    public SpinLock() {
        m_lock = 0;
    }

    // Methods

    /**
     * Gets the offset of the lock field
     *
     * @return the offset of the lock field
     */
    private static long getLockOffset() {
        try {
            return UNSAFE.objectFieldOffset(SpinLock.class.getDeclaredField("m_lock"));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override public void lock() {
        while (!UNSAFE.compareAndSwapInt(this, OFFSET, 0, 1)) {
        }
    }

    @Override public void unlock() {
        UNSAFE.compareAndSwapInt(this, OFFSET, 1, 0);
    }

    @Override public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException("The method 'lockInterruptibly' is not supported.");
    }

    @Override public boolean tryLock() {
        return UNSAFE.compareAndSwapInt(this, OFFSET, 0, 1);
    }

    @Override public boolean tryLock(final long p_time, final TimeUnit p_unit) throws InterruptedException {
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

    @Override public Condition newCondition() {
        throw new UnsupportedOperationException("The method 'newCondition' is not supported.");
    }

}
