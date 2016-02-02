package de.hhu.bsinfo.dxram.job;

public interface RemoteSubmissionDelegate {
	public boolean pushJobRemote(final Job p_job, final short p_nodeID);
}
