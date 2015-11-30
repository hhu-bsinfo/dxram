package de.uniduesseldorf.dxcompute.test;

import de.uniduesseldorf.dxcompute.job.Job;
import de.uniduesseldorf.dxcompute.job.JobInterface;

public class JobTest implements Job
{
	private int m_id;
	private long m_timeToWait;
	
	public JobTest(final int p_id, final long p_timeToWait)
	{
		m_id = p_id;
		m_timeToWait = p_timeToWait;
	}
	
	@Override
	public void execute(JobInterface p_jobApi) 
	{	
		try {
			Thread.sleep(m_timeToWait);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public long getJobID() {
		return m_id;
	}
}
