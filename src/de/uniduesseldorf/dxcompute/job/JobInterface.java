package de.uniduesseldorf.dxcompute.job;

import de.uniduesseldorf.dxcompute.logger.LOG_LEVEL;

public interface JobInterface
{
	public int getAssignedWorkerID();
	
	public boolean pushJobPublicLocalQueue(final Job p_job);
	
	public boolean pushJobPrivateQueue(final Job p_job);
	
	public void log(final LOG_LEVEL p_level, final String p_msg);
}
