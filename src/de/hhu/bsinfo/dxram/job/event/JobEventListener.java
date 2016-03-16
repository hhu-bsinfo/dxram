package de.hhu.bsinfo.dxram.job.event;

public interface JobEventListener {
	public byte getJobEventBitMask();
	
	public void jobEventTriggered(final byte p_eventId, final long p_jobId, final short p_sourceNodeId);
}
