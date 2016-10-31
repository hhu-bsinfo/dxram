
package de.hhu.bsinfo.dxcompute.job.ws;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

import de.hhu.bsinfo.dxcompute.job.AbstractJob;

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
