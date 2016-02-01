package de.hhu.bsinfo.dxcompute.job;

import de.hhu.bsinfo.dxram.logger.LoggerComponent;

public interface WorkerDelegate 
{
	public Job stealJobLocal(final Worker p_thief);
	
	public void scheduledJob();
	
	public void finishedJob();
	
	public LoggerComponent getLoggerComponent();
	
	public short getNodeID();
}
