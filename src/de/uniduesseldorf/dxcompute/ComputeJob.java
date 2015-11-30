package de.uniduesseldorf.dxcompute;

import de.uniduesseldorf.dxcompute.job.Job;
import de.uniduesseldorf.dxcompute.job.JobInterface;

public abstract class ComputeJob extends Job
{
	private StorageInterface m_storageInterface;
	
	public ComputeJob()
	{
		
	}
	
	public void execute(final JobInterface p_jobInterface, final StorageInterface p_storageInterface)
	{
		m_storageInterface = p_storageInterface;
		execute(p_jobInterface);
	}
	
	protected StorageInterface getStorageInterface()
	{
		return m_storageInterface;
	}
}
