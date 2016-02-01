package de.hhu.bsinfo.dxcompute.job;

import de.hhu.bsinfo.dxram.stats.StatisticsRecorder;

public class JobStatisticsRecorderIDs {
	public int m_id = StatisticsRecorder.INVALID_ID;
	public Operations m_operations = new Operations();
	
	public static class Operations {
		public static final String MS_SUBMIT = "Submit";
		public static final String MS_REMOTE_SUBMIT = "RemoteSubmit";
		public static final String MS_INCOMING_SUBMIT = "IncomingSubmit";
		
		public int m_submit = StatisticsRecorder.Operation.INVALID_ID;
		public int m_remoteSubmit = StatisticsRecorder.Operation.INVALID_ID;
		public int m_incomingSubmit = StatisticsRecorder.Operation.INVALID_ID;
	}
}
