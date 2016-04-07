package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.stats.StatisticsRecorder;

/**
 * Metadata needed for registering and recording statistics in ChunkService.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 *
 */
public class ChunkStatisticsRecorderIDs {
	public int m_id = StatisticsRecorder.INVALID_ID;
	public static final Operations OPERATIONS = new Operations();

	/**
	 * List of operations for chunk statistics
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
	 *
	 */
	public static class Operations {
		public static final String MS_CREATE = "Create";
		public static final String MS_REMOTE_CREATE = "RemoteCreate";
		public static final String MS_SIZE = "Size";
		public static final String MS_GET = "Get";
		public static final String MS_PUT = "Put";
		public static final String MS_REMOVE = "Remove";
		public static final String MS_PUT_ASYNC = "PutAsync";
		public static final String MS_INCOMING_CREATE = "IncomingCreate";
		public static final String MS_INCOMING_GET = "IncomingGet";
		public static final String MS_INCOMING_PUT = "IncomingPut";
		public static final String MS_INCOMING_REMOVE = "IncomingRemove";
		public static final String MS_INCOMING_PUT_ASYNC = "IncomingPutAsync";

		public int m_create = StatisticsRecorder.Operation.INVALID_ID;
		public int m_remoteCreate = StatisticsRecorder.Operation.INVALID_ID;
		public int m_size = StatisticsRecorder.Operation.INVALID_ID;
		public int m_get = StatisticsRecorder.Operation.INVALID_ID;
		public int m_put = StatisticsRecorder.Operation.INVALID_ID;
		public int m_remove = StatisticsRecorder.Operation.INVALID_ID;
		public int m_putAsync = StatisticsRecorder.Operation.INVALID_ID;
		public int m_incomingCreate = StatisticsRecorder.Operation.INVALID_ID;
		public int m_incomingGet = StatisticsRecorder.Operation.INVALID_ID;
		public int m_incomingPut = StatisticsRecorder.Operation.INVALID_ID;
		public int m_incomingRemove = StatisticsRecorder.Operation.INVALID_ID;
		public int m_incomingPutAsync = StatisticsRecorder.Operation.INVALID_ID;
	}
}
