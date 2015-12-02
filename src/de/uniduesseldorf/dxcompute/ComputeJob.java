package de.uniduesseldorf.dxcompute;

import de.uniduesseldorf.dxcompute.job.Job;
import de.uniduesseldorf.dxcompute.job.JobDelegate;

public abstract class ComputeJob extends Job
{
	private StorageDelegate m_storageDelegate;
	
	public ComputeJob()
	{
		
	}
	
	void setStorageDelegate(final StorageDelegate p_storageDelegate)
	{
		m_storageDelegate = p_storageDelegate;
	}
	
	protected StorageDelegate getStorageDelegate()
	{
		return m_storageDelegate;
	}
}
