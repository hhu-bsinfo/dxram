/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.job.ws;

import java.util.Deque;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxram.job.AbstractJob;

/**
 * Simple mutex implementation of a work stealing queue.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class WorkStealingQueueMutex implements WorkStealingQueue {
    private Deque<AbstractJob> m_queue = new LinkedList<AbstractJob>();
    private Lock m_lock = new ReentrantLock();

    /**
     * Constructor
     */
    public WorkStealingQueueMutex() {

    }

    // -------------------------------------------------------------------

    @Override
    public int count() {
        return m_queue.size();
    }

    @Override
    public boolean push(final AbstractJob p_job) {
        m_lock.lock();
        m_queue.push(p_job);
        m_lock.unlock();
        return true;
    }

    @Override
    public AbstractJob pop() {
        AbstractJob job = null;

        m_lock.lock();
        try {
            job = m_queue.pop();
        } catch (final NoSuchElementException e) {
            return null;
        } finally {
            m_lock.unlock();
        }
        return job;
    }

    @Override
    public AbstractJob steal() {
        AbstractJob job = null;

        m_lock.lock();
        try {
            job = m_queue.removeFirst();
        } catch (final NoSuchElementException e) {
            return null;
        } finally {
            m_lock.unlock();
        }

        return job;
    }
}
