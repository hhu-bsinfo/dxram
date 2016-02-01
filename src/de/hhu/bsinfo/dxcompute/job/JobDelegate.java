package de.hhu.bsinfo.dxcompute.job;

public interface JobDelegate
{
	public int getAssignedWorkerID();
	
	public boolean pushJobPublicRemoteQueue(final Job p_job, final short p_nodeID);
	
	public boolean pushJobPublicLocalQueue(final Job p_job);
	
	public boolean pushJobPrivateQueue(final Job p_job);
	
	// provide a very simple log interface only
	public void log(final Job p_job, final String p_msg);
}
