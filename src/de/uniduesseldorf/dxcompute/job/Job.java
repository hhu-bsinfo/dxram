package de.uniduesseldorf.dxcompute.job;

public abstract class Job 
{
	private JobInterface m_jobInterface;
	
	public Job()
	{
		
	}
	
	public void execute(final JobInterface p_jobInterface)
	{
		m_jobInterface = p_jobInterface;
		execute();
	}
	
	protected abstract void execute();
	
	public abstract long getJobID();
	
	protected JobInterface getJobInterface()
	{
		return m_jobInterface;
	}
}
