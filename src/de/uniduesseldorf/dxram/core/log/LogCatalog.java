
package de.uniduesseldorf.dxram.core.log;

import java.io.IOException;
import java.util.ArrayList;

import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.log.storage.SecondaryLogBuffer;
import de.uniduesseldorf.dxram.core.log.storage.SecondaryLogWithSegments;

/**
 * Log catalog: Bundles all logs and buffers for one node
 * @author Kevin Beineke 22.05.2015
 */
public final class LogCatalog {

	// Attributes
	private ArrayList<SecondaryLogWithSegments> m_logs;
	private ArrayList<SecondaryLogBuffer> m_buffers;
	private ArrayList<Long> m_backupRanges;

	// Constructors
	/**
	 * Creates an instance of SecondaryLogsReorgThread
	 */
	public LogCatalog() {
		m_logs = new ArrayList<SecondaryLogWithSegments>();
		m_buffers = new ArrayList<SecondaryLogBuffer>();
		m_backupRanges = new ArrayList<Long>();
	}

	// Getter
	/**
	 * Gets the corresponding secondary log
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the secondary log
	 */
	public SecondaryLogWithSegments getLog(final long p_chunkID) {
		SecondaryLogWithSegments ret;
		int rangeID;

		rangeID = getRangeID(p_chunkID);
		ret = m_logs.get(rangeID);
		if (ret == null) {
			System.out.println("ERROR: No secondary log for " + p_chunkID);
		}

		return ret;
	}

	/**
	 * Gets the corresponding secondary log buffer
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the secondary log buffer
	 */
	public SecondaryLogBuffer getBuffer(final long p_chunkID) {
		SecondaryLogBuffer ret;
		int rangeID;

		rangeID = getRangeID(p_chunkID);
		ret = m_buffers.get(rangeID);
		if (ret == null) {
			System.out.println("ERROR: No secondary log buffer for " + p_chunkID);
		}

		return ret;
	}

	/**
	 * Gets the corresponding range
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the first ChunkID of the range
	 */
	public long getRange(final long p_chunkID) {
		long ret = -1;

		for (int i = m_logs.size() - 1; i >= 0; i--) {
			if (m_backupRanges.get(i) <= ChunkID.getLocalID(p_chunkID)) {
				ret = m_backupRanges.get(i);
			}
		}

		return ret;
	}

	/**
	 * Gets all secondary logs from this node
	 * @return the secondary log array
	 */
	public SecondaryLogWithSegments[] getAllLogs() {
		SecondaryLogWithSegments[] ret = null;

		for (int i = 0; i < m_backupRanges.size(); i++) {
			System.out.println(m_backupRanges.get(i));
		}

		ret = m_logs.toArray(new SecondaryLogWithSegments[m_logs.size()]);

		return ret;
	}

	// Setter
	/**
	 * Inserts a new range
	 * @param p_low
	 *            the first ChunkID of the range
	 * @param p_log
	 *            the new secondary log to link
	 * @throws IOException
	 *             if no new secondary log could be created
	 * @throws InterruptedException
	 *             if no new secondary log could be created
	 */
	public void insertRange(final long p_low, final SecondaryLogWithSegments p_log) throws IOException,
	InterruptedException {
		SecondaryLogBuffer buffer;
		int rangeID;

		rangeID = m_backupRanges.size();
		m_logs.add(rangeID, p_log);

		// Create new secondary log buffer
		buffer = new SecondaryLogBuffer(p_log);
		m_buffers.add(rangeID, buffer);

		// Insert range
		m_backupRanges.add(ChunkID.getLocalID(p_low));
	}

	/**
	 * Determines the corresponding range
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the RangeID
	 */
	private int getRangeID(final long p_chunkID) {
		int ret = 0;

		for (int i = m_logs.size() - 1; i >= 0; i--) {
			if (m_backupRanges.get(i) <= ChunkID.getLocalID(p_chunkID)) {
				ret = i;
			}
		}

		return ret;
	}

	/**
	 * Closes all logs and buffers from this node
	 * @throws IOException
	 *             if the log could not be closed
	 * @throws InterruptedException
	 *             if the log could not be closed
	 */
	public void closeLogsAndBuffers() throws IOException, InterruptedException {
		for (int i = 0; i < m_logs.size(); i++) {
			m_buffers.get(i).close();
			m_logs.get(i).closeLog();
		}
		m_buffers = null;
		m_logs = null;
		m_backupRanges = null;
	}
}
