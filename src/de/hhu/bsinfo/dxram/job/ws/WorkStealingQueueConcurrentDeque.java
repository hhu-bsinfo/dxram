/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.job.ws;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

import de.hhu.bsinfo.dxram.job.AbstractJob;

/**
 * Work stealing queue implementation using Java's ConcurrentLinkedDeque
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 14.03.2016
 */
public class WorkStealingQueueConcurrentDeque implements WorkStealingQueue {
    // keeping track of the queue size because the size
    // call on the queue yields the worst performance possible
    private AtomicInteger m_queueCount = new AtomicInteger(0);
    private ConcurrentLinkedDeque<AbstractJob> m_queue = new ConcurrentLinkedDeque<AbstractJob>();

    /**
     * Constructor
     */
    public WorkStealingQueueConcurrentDeque() {

    }

    // -------------------------------------------------------------------

    @Override
    public int count() {
        return m_queueCount.get();
    }

    @Override
    public boolean push(final AbstractJob p_job) {
        // queue always returns true
        m_queueCount.incrementAndGet();
        return m_queue.add(p_job);
    }

    @Override
    public AbstractJob pop() {
        AbstractJob job = m_queue.pollLast();
        if (job != null) {
            m_queueCount.decrementAndGet();
        }
        return job;
    }

    @Override
    public AbstractJob steal() {
        AbstractJob job = m_queue.pollFirst();
        if (job != null) {
            m_queueCount.decrementAndGet();
        }
        return job;
    }
}
