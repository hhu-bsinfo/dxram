package de.uniduesseldorf.dxcompute.job;

public interface JobDelegate
{
	public int getAssignedWorkerID();
	
	public boolean pushJobPublicLocalQueue(final Job p_job);
	
	public boolean pushJobPrivateQueue(final Job p_job);
}
