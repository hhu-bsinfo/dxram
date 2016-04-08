
package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.dxram.stats.StatisticsRecorder;

/**
 * Metadata needed for registering and recording statistics in ChunkService.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 */
public class MemoryStatisticsRecorderIDs {
	public int m_id = StatisticsRecorder.INVALID_ID;
	public Operations m_operations = new Operations();

	/**
	 * List of operations for memory statistics
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
	 */
	public static class Operations {
		public static final String MS_CREATE_NID_TABLE = "CreateNIDTable";
		public static final String MS_CREATE_LID_TABLE = "CreateLIDTable";
		public static final String MS_MALLOC = "Malloc";
		public static final String MS_FREE = "Free";
		public static final String MS_GET = "Get";
		public static final String MS_PUT = "Put";
		public static final String MS_CREATE = "Create";
		public static final String MS_REMOVE = "Remove";

		public int m_createNIDTable = StatisticsRecorder.Operation.INVALID_ID;
		public int m_createLIDTable = StatisticsRecorder.Operation.INVALID_ID;
		public int m_malloc = StatisticsRecorder.Operation.INVALID_ID;
		public int m_free = StatisticsRecorder.Operation.INVALID_ID;
		public int m_get = StatisticsRecorder.Operation.INVALID_ID;
		public int m_put = StatisticsRecorder.Operation.INVALID_ID;
		public int m_create = StatisticsRecorder.Operation.INVALID_ID;
		public int m_remove = StatisticsRecorder.Operation.INVALID_ID;
	}
}
