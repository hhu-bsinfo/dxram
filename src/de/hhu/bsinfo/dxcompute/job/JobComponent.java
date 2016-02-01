package de.hhu.bsinfo.dxcompute.job;

import de.hhu.bsinfo.dxram.engine.DXRAMComponent;

public abstract class JobComponent extends DXRAMComponent {

	public JobComponent(int p_priorityInit, int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}
	
	public abstract void setRemoteSubsmissionDelegate(final RemoteSubmissionDelegate p_delegate);

	public abstract boolean submit(final Job p_job);
	
	public abstract long getNumberOfUnfinishedJobs();
	
	public abstract void waitForSubmittedJobsToFinish();
}
