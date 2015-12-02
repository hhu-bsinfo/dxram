package de.uniduesseldorf.dxcompute.job;

import java.util.Deque;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class WorkStealingQueue 
{
	private Deque<Job> m_queue = new LinkedList<Job>();
	private Lock m_lock = new ReentrantLock();
	
	public WorkStealingQueue()
	{
		
	}
	
	// -------------------------------------------------------------------
	
	public int jobsScheduled()
	{
		return m_queue.size();
	}
	
	public boolean push(final Job job)
	{
		m_lock.lock();
		m_queue.push(job);
		m_lock.unlock();
		return true;
	}
	
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
