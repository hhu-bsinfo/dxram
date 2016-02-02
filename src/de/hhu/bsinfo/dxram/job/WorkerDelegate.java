package de.hhu.bsinfo.dxram.job;

import de.hhu.bsinfo.dxram.engine.DXRAMService;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;

public interface WorkerDelegate 
{
	public Job stealJobLocal(final Worker p_thief);
	
	public void scheduledJob(final Job p_job);
	
	public void finishedJob(final Job p_job);
	
	public LoggerComponent getLoggerComponent();
	
	public short getNodeID();
	
	public <T extends DXRAMService> T getService(Class<T> p_class);
}
