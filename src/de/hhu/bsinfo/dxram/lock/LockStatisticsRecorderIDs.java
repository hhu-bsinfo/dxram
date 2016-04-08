
package de.hhu.bsinfo.dxram.lock;

import de.hhu.bsinfo.dxram.stats.StatisticsRecorder;

/**
 * Metadata needed for registering and recording statistics in LockService.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 */
public class LockStatisticsRecorderIDs {
	public int m_id = StatisticsRecorder.INVALID_ID;
	public Operations m_operations = new Operations();

	/**
	 * List of operations for chunk statistics
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
	 */
	public static class Operations {
		public static final String MS_LOCK = "Lock";
		public static final String MS_UNLOCK = "Unlock";
		public static final String MS_INCOMING_LOCK = "IncomingLock";
		public static final String MS_INCOMING_UNLOCK = "IncomingUnlock";

		public int m_lock = StatisticsRecorder.Operation.INVALID_ID;
		public int m_unlock = StatisticsRecorder.Operation.INVALID_ID;
		public int m_incomingLock = StatisticsRecorder.Operation.INVALID_ID;
		public int m_incomingUnlock = StatisticsRecorder.Operation.INVALID_ID;
	}
}
