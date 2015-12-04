package de.uniduesseldorf.dxcompute;

import de.uniduesseldorf.dxcompute.data.ComputeJob;

public interface TaskDelegate 
{
	public void submitJob(final ComputeJob p_job);
	
	public void waitForSubmittedJobsToFinish();
}
