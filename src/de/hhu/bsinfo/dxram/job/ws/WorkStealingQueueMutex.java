package de.hhu.bsinfo.dxram.job.ws;

import java.util.Deque;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxram.job.Job;

/**
 * Simple mutex implementation of a work stealing queue.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 */
public class WorkStealingQueueMutex implements WorkStealingQueue
{
	private Deque<Job> m_queue = new LinkedList<Job>();
	private Lock m_lock = new ReentrantLock();
	
	/**
	 * Constructor
	 */
	public WorkStealingQueueMutex()
	{
		
	}
	
	// -------------------------------------------------------------------
	
	@Override
	public int count()
	{
		return m_queue.size();
	}
	
	@Override
	public boolean push(final Job job)
	{
		m_lock.lock();
		m_queue.push(job);
		m_lock.unlock();
		return true;
	}
	
	@Override
	public Job pop()
	{
		Job job = null;
		
		m_lock.lock();
		try {
			job = m_queue.pop();
		} catch (NoSuchElementException e) {
			return null;
		}
		finally {
			m_lock.unlock();
		}
		return job;
	}
	
	@Override
	public Job steal()
	{
		Job job = null;
		
		m_lock.lock();
		try {
			job = m_queue.removeFirst();
		} catch (NoSuchElementException e) {
			return null;
		} finally {
			m_lock.unlock();
		}
		
		return job;
	}
}
