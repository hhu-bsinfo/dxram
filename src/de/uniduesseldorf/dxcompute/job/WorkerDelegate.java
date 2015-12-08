package de.uniduesseldorf.dxcompute.job;

public interface WorkerDelegate 
{
	public Job stealJobLocal(final Worker p_thief);
	
	public void scheduledJob();
	
	public void finishedJob();
}
