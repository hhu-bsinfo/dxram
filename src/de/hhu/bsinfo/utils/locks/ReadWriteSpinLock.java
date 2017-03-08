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
import java.util.concurrent.locks.ReadWriteLock;

import de.hhu.bsinfo.utils.UnsafeHandler;

/**
 * Represents a spinlock
 *
 * @author Florian Klein, florian.klein@hhu.de, 05.04.2014
 */
public final class ReadWriteSpinLock implements ReadWriteLock {

    // Constants
    @SuppressWarnings("sunapi")
    private static final sun.misc.Unsafe UNSAFE = UnsafeHandler.getInstance().getUnsafe();
    private static final long OFFSET = getLockOffset();

    private static final int READER_BITMASK = 0x7FFFFFFF;
    private static final int WRITER_FLAG = 0x80000000;

    // Attributes
    private volatile int m_lock;
    private ReadLock m_readLock;
    private WriteLock m_writeLock;

    // Constructors

    /**
     * Creates an instance of ReadWriteSpinLock
     */
    public ReadWriteSpinLock() {
        m_lock = 0;
        m_readLock = new ReadLock();
        m_writeLock = new WriteLock();
    }

    // Methods

    /**
     * Gets the offset of the lock field
     *
     * @return the offset of the lock field
     */
    private static long getLockOffset() {
        try {
            return UNSAFE.objectFieldOffset(ReadWriteSpinLock.class.getDeclaredField("m_lock"));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override public Lock readLock() {
        return m_readLock;
    }

    @Override public Lock writeLock() {
        return m_writeLock;
    }

    // Classes

    /**
     * Represent the ReadLock part
     *
     * @author Florian Klein 27.10.2014
     */
    private final class ReadLock implements Lock {

        // Constructors

        /**
         * Creates an instance of ReadLock
         */
        private ReadLock() {
        }

        // Methods
        @Override public void lock() {
            int lock;

            while ((m_lock & WRITER_FLAG) != 0) {
            }

            do {
                lock = m_lock & READER_BITMASK;
            } while (!UNSAFE.compareAndSwapInt(this, OFFSET, lock, lock + 1));
        }

        @Override public void unlock() {
            int lock;

            do {
                lock = m_lock;
            } while (!UNSAFE.compareAndSwapInt(this, OFFSET, lock, lock - 1));
        }

        @Override public void lockInterruptibly() throws InterruptedException {
            throw new UnsupportedOperationException("The method 'lockInterruptibly' is not supported.");
        }

        @Override public boolean tryLock() {
            boolean ret = false;
            int lock;

            if ((m_lock & WRITER_FLAG) == 0) {
                lock = m_lock & READER_BITMASK;
                ret = UNSAFE.compareAndSwapInt(this, OFFSET, lock, lock + 1);
            }

            return ret;
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

    /**
     * Represent the WriteLock part
     *
     * @author Florian Klein 27.10.2014
     */
    private final class WriteLock implements Lock {

        // Constructors

        /**
         * Creates an instance of WriteLock
         */
        private WriteLock() {
        }

        // Methods
        @Override public void lock() {
            int lock;

            do {
                lock = m_lock & READER_BITMASK;
            } while (!UNSAFE.compareAndSwapInt(this, OFFSET, lock, lock | WRITER_FLAG));

            while ((m_lock & READER_BITMASK) != 0) {
            }
        }

        @Override public void unlock() {
            UNSAFE.compareAndSwapInt(this, OFFSET, WRITER_FLAG, 0);
        }

        @Override public void lockInterruptibly() throws InterruptedException {
            throw new UnsupportedOperationException("The method 'lockInterruptibly' is not supported.");
        }

        @Override public boolean tryLock() {
            return UNSAFE.compareAndSwapInt(this, OFFSET, 0, WRITER_FLAG);
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

}
