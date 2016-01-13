package de.hhu.bsinfo.dxcompute;

public interface TaskDelegate 
{
	public void submitJob(final ComputeJob p_job);
	
	public void waitForSubmittedJobsToFinish();
}
