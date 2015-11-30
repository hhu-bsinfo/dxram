package de.uniduesseldorf.dxcompute;

import de.uniduesseldorf.dxcompute.job.JobSystem;
import de.uniduesseldorf.dxcompute.logger.LoggerInterface;

public class DXCompute 
{
	private StorageInterface m_storageInterface;
	
	private JobSystem m_jobSystem;
	
	public DXCompute(final LoggerInterface p_loggerInterface)
	{
		m_jobSystem = new JobSystem("DXCompute", p_loggerInterface);
	}
	
	public boolean init(final int p_numThreads, final StorageInterface p_storageInterface)
	{
		m_storageInterface = p_storageInterface;
	
		m_jobSystem.init(p_numThreads);
		
		return true;
	}
	
	public void execute(final TaskPipeline p_taskPipeline)
	{
		p_taskPipeline.execute();
	}
	
	public void shutdown()
	{
		
	}
}
