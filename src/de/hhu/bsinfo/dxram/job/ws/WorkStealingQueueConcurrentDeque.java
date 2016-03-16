package de.hhu.bsinfo.dxram.job.ws;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

import de.hhu.bsinfo.dxram.job.Job;

/**
 * Work stealing queue implementation using Java's ConcurrentLinkedDeque
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 14.03.16
 */
public class WorkStealingQueueConcurrentDeque implements WorkStealingQueue
{
	// keeping track of the queue size because the size
	// call on the queue yields the worst performance possible
	private AtomicInteger m_queueCount = new AtomicInteger(0);
	private ConcurrentLinkedDeque<Job> m_queue = new ConcurrentLinkedDeque<Job>();
	
	/**
	 * Constructor
	 */
	public WorkStealingQueueConcurrentDeque()
	{
		
	}
	
	// -------------------------------------------------------------------
	
	@Override
	public int count()
	{
		return m_queueCount.get();
	}
	
	@Override
	public boolean push(final Job job)
	{
		// queue always returns true
		m_queueCount.incrementAndGet();
		return m_queue.add(job);
	}
	
	@Override
	public Job pop()
	{
		Job job = m_queue.pollLast();
		if (job != null)
			m_queueCount.decrementAndGet();
		return job;
	}
	
	@Override
	public Job steal()
	{
		Job job = m_queue.pollFirst();
		if (job != null)
			m_queueCount.decrementAndGet();
		return job;
	}
}
