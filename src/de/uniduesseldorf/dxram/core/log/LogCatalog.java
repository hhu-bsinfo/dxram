
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
	private ArrayList<SecondaryLogWithSegments> m_creatorLogs;
	private ArrayList<SecondaryLogBuffer> m_creatorBuffers;
	private ArrayList<Long> m_creatorBackupRanges;

	private ArrayList<SecondaryLogWithSegments> m_migrationLogs;
	private ArrayList<SecondaryLogBuffer> m_migrationBuffers;
	private ArrayList<Long> m_migrationBackupRanges;

	// Constructors
	/**
	 * Creates an instance of SecondaryLogsReorgThread
	 */
	public LogCatalog() {
		m_creatorLogs = new ArrayList<SecondaryLogWithSegments>();
		m_creatorBuffers = new ArrayList<SecondaryLogBuffer>();
		m_creatorBackupRanges = new ArrayList<Long>();

		m_migrationLogs = new ArrayList<SecondaryLogWithSegments>();
		m_migrationBuffers = new ArrayList<SecondaryLogBuffer>();
		m_migrationBackupRanges = new ArrayList<Long>();
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
		ret = m_creatorLogs.get(rangeID);
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
		ret = m_creatorBuffers.get(rangeID);
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

		for (int i = m_creatorLogs.size() - 1; i >= 0; i--) {
			if (m_creatorBackupRanges.get(i) <= ChunkID.getLocalID(p_chunkID)) {
				ret = m_creatorBackupRanges.get(i);
				break;
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

		ret = m_creatorLogs.toArray(new SecondaryLogWithSegments[m_creatorLogs.size()]);

		return ret;
	}

	/**
	 * Gets all secondary log buffers from this node
	 * @return the secondary log buffer array
	 */
	public SecondaryLogBuffer[] getAllBuffers() {
		SecondaryLogBuffer[] ret = null;

		ret = m_creatorBuffers.toArray(new SecondaryLogBuffer[m_creatorBuffers.size()]);

		return ret;
	}

	// Setter
	/**
	 * Inserts a new range
	 * @param p_firstChunkIDOrRangeID
	 *            the first ChunkID of the range
	 * @param p_log
	 *            the new secondary log to link
	 * @throws IOException
	 *             if no new secondary log could be created
	 * @throws InterruptedException
	 *             if no new secondary log could be created
	 */
	public void insertRange(final long p_firstChunkIDOrRangeID, final SecondaryLogWithSegments p_log)
			throws IOException,
			InterruptedException {
		SecondaryLogBuffer buffer;
		int rangeID;

		if (ChunkID.getCreatorID(p_firstChunkIDOrRangeID) != -1) {
			rangeID = m_creatorBackupRanges.size();
			m_creatorLogs.add(rangeID, p_log);

			// Create new secondary log buffer
			buffer = new SecondaryLogBuffer(p_log);
			m_creatorBuffers.add(rangeID, buffer);

			// Insert range
			m_creatorBackupRanges.add(ChunkID.getLocalID(p_firstChunkIDOrRangeID));
		} else {
			rangeID = m_migrationBackupRanges.size();
			m_migrationLogs.add(rangeID, p_log);

			// Create new secondary log buffer
			buffer = new SecondaryLogBuffer(p_log);
			m_migrationBuffers.add(rangeID, buffer);

			// Insert range
			m_creatorBackupRanges.add(ChunkID.getLocalID(p_firstChunkIDOrRangeID));
		}
	}

	/**
	 * Determines the corresponding range
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the RangeID
	 */
	private int getRangeID(final long p_chunkID) {
		int ret = 0;

		for (int i = m_creatorLogs.size() - 1; i >= 0; i--) {
			if (m_creatorBackupRanges.get(i) <= ChunkID.getLocalID(p_chunkID)) {
				ret = i;
				break;
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
		for (int i = 0; i < m_creatorLogs.size(); i++) {
			m_creatorBuffers.get(i).close();
			m_creatorLogs.get(i).closeLog();
		}
		m_creatorBuffers = null;
		m_creatorLogs = null;
		m_creatorBackupRanges = null;
	}
}
