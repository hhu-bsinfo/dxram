
package de.uniduesseldorf.dxram.core.log.storage;

import java.io.IOException;
import java.util.ArrayList;

import de.uniduesseldorf.dxram.core.api.ChunkID;

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
	private int m_currentRangeID;

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
		m_currentRangeID = 0;
	}

	// Getter
	/**
	 * Gets the corresponding secondary log
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_rangeID
	 *            the RangeID for migrations or -1
	 * @return the secondary log
	 */
	public SecondaryLogWithSegments getLog(final long p_chunkID, final byte p_rangeID) {
		SecondaryLogWithSegments ret;
		int rangeID;

		if (p_rangeID != -1) {
			ret = m_migrationLogs.get(p_rangeID);
		} else {
			rangeID = getRangeID(p_chunkID);
			ret = m_creatorLogs.get(rangeID);
		}
		if (ret == null) {
			System.out.println("ERROR: No secondary log for " + p_chunkID);
		}

		return ret;
	}

	/**
	 * Gets the corresponding secondary log buffer
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_rangeID
	 *            the RangeID for migrations or -1
	 * @return the secondary log buffer
	 */
	public SecondaryLogBuffer getBuffer(final long p_chunkID, final byte p_rangeID) {
		SecondaryLogBuffer ret;
		int rangeID;

		if (p_rangeID != -1) {
			ret = m_migrationBuffers.get(p_rangeID);
		} else {
			rangeID = getRangeID(p_chunkID);
			ret = m_creatorBuffers.get(rangeID);
		}
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
		SecondaryLogWithSegments[] ret;
		SecondaryLogWithSegments[] creatorLogs;
		SecondaryLogWithSegments[] migrationLogs;

		creatorLogs = m_creatorLogs.toArray(new SecondaryLogWithSegments[m_creatorLogs.size()]);
		migrationLogs = m_migrationLogs.toArray(new SecondaryLogWithSegments[m_migrationLogs.size()]);

		ret = new SecondaryLogWithSegments[creatorLogs.length + migrationLogs.length];
		System.arraycopy(creatorLogs, 0, ret, 0, creatorLogs.length);
		System.arraycopy(migrationLogs, 0, ret, creatorLogs.length, migrationLogs.length);

		return ret;
	}

	/**
	 * Gets all creator secondary logs from this node
	 * @return the creator secondary log array
	 */
	public SecondaryLogWithSegments[] getAllCreatorLogs() {
		return m_creatorLogs.toArray(new SecondaryLogWithSegments[m_creatorLogs.size()]);
	}

	/**
	 * Gets all migration secondary logs from this node
	 * @return the migration secondary log array
	 */
	public SecondaryLogWithSegments[] getAllMigrationLogs() {
		return m_migrationLogs.toArray(new SecondaryLogWithSegments[m_migrationLogs.size()]);
	}

	/**
	 * Gets all secondary log buffers from this node
	 * @return the secondary log buffer array
	 */
	public SecondaryLogBuffer[] getAllBuffers() {
		SecondaryLogBuffer[] ret;
		SecondaryLogBuffer[] creatorBuffers;
		SecondaryLogBuffer[] migrationBuffers;

		creatorBuffers = m_creatorBuffers.toArray(new SecondaryLogBuffer[m_creatorBuffers.size()]);
		migrationBuffers = m_migrationBuffers.toArray(new SecondaryLogBuffer[m_migrationBuffers.size()]);

		ret = new SecondaryLogBuffer[creatorBuffers.length + migrationBuffers.length];
		System.arraycopy(creatorBuffers, 0, ret, 0, creatorBuffers.length);
		System.arraycopy(migrationBuffers, 0, ret, creatorBuffers.length, migrationBuffers.length);

		return ret;
	}

	/**
	 * Gets all creator secondary log buffers from this node
	 * @return the creator secondary log buffer array
	 */
	public SecondaryLogBuffer[] getAllCreatorBuffers() {
		return m_creatorBuffers.toArray(new SecondaryLogBuffer[m_creatorBuffers.size()]);
	}

	/**
	 * Gets all migration secondary log buffers from this node
	 * @return the migration secondary log buffer array
	 */
	public SecondaryLogBuffer[] getAllMigrationBuffers() {
		return m_migrationBuffers.toArray(new SecondaryLogBuffer[m_migrationBuffers.size()]);
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
	public void insertRange(final long p_firstChunkIDOrRangeID, final SecondaryLogWithSegments p_log) throws IOException, InterruptedException {
		SecondaryLogBuffer buffer;
		int rangeID;

		if (ChunkID.getCreatorID(p_firstChunkIDOrRangeID) != -1) {
			rangeID = m_creatorBackupRanges.size();
			m_creatorLogs.add(rangeID, p_log);

			// Create new secondary log buffer
			buffer = new SecondaryLogBuffer(p_log, false);
			m_creatorBuffers.add(rangeID, buffer);

			// Insert range
			m_creatorBackupRanges.add(ChunkID.getLocalID(p_firstChunkIDOrRangeID));
		} else {
			m_migrationLogs.add(m_currentRangeID, p_log);

			// Create new secondary log buffer
			buffer = new SecondaryLogBuffer(p_log, true);
			m_migrationBuffers.add(m_currentRangeID, buffer);

			m_currentRangeID++;
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

		for (int i = 0; i < m_migrationLogs.size(); i++) {
			m_migrationBuffers.get(i).close();
			m_migrationLogs.get(i).closeLog();
		}
		m_migrationBuffers = null;
		m_migrationLogs = null;
	}
}
