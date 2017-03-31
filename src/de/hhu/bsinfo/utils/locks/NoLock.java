/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

/**
 * Dummy lock (no real lock)
 *
 * @author Florian Klein, florian.klein@hhu.de, 05.04.2014
 */
public final class NoLock implements Lock {

    // Constructors

    /**
     * Creates an instance of NoLock
     */
    public NoLock() {
    }

    // Methods
    @Override public void lock() {
    }

    @Override public void unlock() {
    }

    @Override public void lockInterruptibly() throws InterruptedException {
    }

    @Override public boolean tryLock() {
        return true;
    }

    @Override public boolean tryLock(final long p_time, final TimeUnit p_unit) throws InterruptedException {
        return true;
    }

    @Override public Condition newCondition() {
        return null;
    }

}
