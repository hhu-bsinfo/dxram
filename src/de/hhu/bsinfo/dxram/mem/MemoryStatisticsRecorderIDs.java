package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.dxram.stats.StatisticsRecorder;

/**
 * Metadata needed for registering and recording statistics in ChunkService.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 *
 */
public class MemoryStatisticsRecorderIDs {
	public int m_id = StatisticsRecorder.INVALID_ID;
	public Operations m_operations = new Operations();
	
	public static class Operations {
		public static final String MS_CREATE_NID_TABLE = "CreateNIDTable";
		public static final String MS_CREATE_LID_TABLE = "CreateLIDTable";
		public static final String MS_MALLOC = "Malloc";
		public static final String MS_FREE = "Free";
		
		public int m_createNIDTable = StatisticsRecorder.Operation.INVALID_ID;
		public int m_createLIDTable = StatisticsRecorder.Operation.INVALID_ID;
		public int m_malloc = StatisticsRecorder.Operation.INVALID_ID;
		public int m_free = StatisticsRecorder.Operation.INVALID_ID;
	}
}
