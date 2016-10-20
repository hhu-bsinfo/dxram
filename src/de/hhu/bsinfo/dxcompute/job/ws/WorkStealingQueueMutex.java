
package de.hhu.bsinfo.dxcompute.job.ws;

import java.util.Deque;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxcompute.job.AbstractJob;

/**
 * Simple mutex implementation of a work stealing queue.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
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
