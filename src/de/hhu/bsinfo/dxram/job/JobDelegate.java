package de.hhu.bsinfo.dxram.job;

import de.hhu.bsinfo.dxram.engine.DXRAMService;

public interface JobDelegate
{
//	public int getAssignedWorkerID();
//	
//	public boolean pushJobPublicRemoteQueue(final Job p_job, final short p_nodeID);
//	
//	public boolean pushJobPublicLocalQueue(final Job p_job);
//	
//	public boolean pushJobPrivateQueue(final Job p_job);
	
	// so this kinda breaks the idea "services do not know other services"
	// but a job has to be seen as code by the user "from outside" of the system
	// so he needs access to certain services obviously in order to get his job done
	// make sure to forbid the JobService
	public <T extends DXRAMService> T getService(final Class<T> p_class); 
	
//	// provide a very simple log interface only
//	public void log(final Job p_job, final String p_msg);
}
