package de.hhu.bsinfo.dxcompute.test;

import de.hhu.bsinfo.dxcompute.ComputeJob;

public class JobTest extends ComputeJob
{
	private long m_timeToWait;
	
	public JobTest(final long p_timeToWait)
	{
		m_timeToWait = p_timeToWait;
	}
	
	@Override
	protected void execute() 
	{	
		try {
			log("Sleeping " + m_timeToWait);
			Thread.sleep(m_timeToWait);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
