
package de.hhu.bsinfo.dxcompute.job;

import de.hhu.bsinfo.dxram.stats.StatisticsRecorder;

/**
 * Metadata needed for registering and recording statistics in JobService.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 */
public class JobStatisticsRecorderIDs {
	public int m_id = StatisticsRecorder.INVALID_ID;
	public Operations m_operations = new Operations();

	/**
	 * Operations
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
	 */
	public static class Operations {
		public static final String MS_SUBMIT = "Submit";
		public static final String MS_REMOTE_SUBMIT = "RemoteSubmit";
		public static final String MS_INCOMING_SUBMIT = "IncomingSubmit";

		public int m_submit = StatisticsRecorder.Operation.INVALID_ID;
		public int m_remoteSubmit = StatisticsRecorder.Operation.INVALID_ID;
		public int m_incomingSubmit = StatisticsRecorder.Operation.INVALID_ID;
	}
}
