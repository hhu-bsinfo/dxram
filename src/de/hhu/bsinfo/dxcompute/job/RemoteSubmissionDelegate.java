package de.hhu.bsinfo.dxcompute.job;

public interface RemoteSubmissionDelegate {
	public boolean pushJobRemoteQueue(final Job p_job, final short p_nodeID);
}
