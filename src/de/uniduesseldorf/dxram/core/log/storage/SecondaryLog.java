
package de.uniduesseldorf.dxram.core.log.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.log.EpochVersion;
import de.uniduesseldorf.dxram.core.log.LogHandler.SecondaryLogsReorgThread;
import de.uniduesseldorf.dxram.core.log.header.AbstractLogEntryHeader;
import de.uniduesseldorf.dxram.core.util.ChunkID;
import de.uniduesseldorf.dxram.core.util.NodeID;

/**
 * This class implements the secondary log
 * @author Kevin Beineke 23.10.2014
 */
public class SecondaryLog extends AbstractLog {

	// Constants
	private static final String BACKUP_DIRECTORY = Core.getConfiguration().getStringValue(ConfigurationConstants.LOG_DIRECTORY);
	private static final String SECLOG_PREFIX_FILENAME = "sec";
	private static final String SECLOG_POSTFIX_FILENAME = ".log";
	private static final boolean USE_CHECKSUM = Core.getConfiguration().getBooleanValue(ConfigurationConstants.LOG_CHECKSUM);
	private static final byte[] SECLOG_HEADER = "DXRAMSecLogv1".getBytes(Charset.forName("UTF-8"));

	private static final long SECLOG_SIZE = Core.getConfiguration().getLongValue(ConfigurationConstants.SECONDARY_LOG_SIZE);
	private static final int SECLOG_MIN_SIZE = 1024 * FLASHPAGE_SIZE;
	private static final int REORG_UTILIZATION_THRESHOLD = Core.getConfiguration().getIntValue(ConfigurationConstants.REORG_UTILIZATION_THRESHOLD);
	private static final int SECLOG_SEGMENT_SIZE = Core.getConfiguration().getIntValue(ConfigurationConstants.LOG_SEGMENT_SIZE);

	// Attributes
	private short m_nodeID;
	private long m_rangeIDOrFirstLocalID;

	private VersionsHashTableRAM m_hashTable;

	private long m_numberOfBytes;
	private int m_numberOfInvalidsInLog;

	private long m_secondaryLogReorgThreshold;
	private SecondaryLogsReorgThread m_reorganizationThread;

	private SegmentHeader[] m_segmentHeaders;
	private byte m_numberOfFreeSegments;

	private boolean m_isAccessed;
	private SegmentHeader m_activeSegment;
	private ReentrantLock m_lock;

	private boolean m_storesMigrations;

	// Constructors
	/**
	 * Creates an instance of SecondaryLog with default configuration except
	 * secondary log size
	 * @param p_reorganizationThread
	 *            the reorganization thread
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_rangeIDOrFirstLocalID
	 *            the RangeID (for migrations) or the first localID of the backup range
	 * @param p_rangeIdentification
	 *            the unique identification of this backup range
	 * @param p_storesMigrations
	 *            whether this secondary log stores migrations or not
	 * @throws IOException
	 *             if secondary log could not be created
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 */
	public SecondaryLog(final SecondaryLogsReorgThread p_reorganizationThread, final short p_nodeID, final long p_rangeIDOrFirstLocalID,
			final String p_rangeIdentification, final boolean p_storesMigrations) throws IOException, InterruptedException {
		super(new File(BACKUP_DIRECTORY + "N" + NodeID.getLocalNodeID() + "_" + SECLOG_PREFIX_FILENAME + p_nodeID + "_" + p_rangeIdentification
				+ SECLOG_POSTFIX_FILENAME), SECLOG_SIZE, SECLOG_HEADER.length);
		if (SECLOG_SIZE < SECLOG_MIN_SIZE) {
			throw new IllegalArgumentException("Error: Secondary log too small");
		}

		m_storesMigrations = p_storesMigrations;
		m_lock = new ReentrantLock(false);

		m_nodeID = p_nodeID;
		m_rangeIDOrFirstLocalID = p_rangeIDOrFirstLocalID;

		m_numberOfBytes = 0;
		m_numberOfInvalidsInLog = 0;

		m_hashTable = new VersionsHashTableRAM(4000000, 0.9f);

		m_reorganizationThread = p_reorganizationThread;

		m_secondaryLogReorgThreshold = (int) (SECLOG_SIZE * ((double) REORG_UTILIZATION_THRESHOLD / 100));

		m_segmentHeaders = new SegmentHeader[(int) (SECLOG_SIZE / SECLOG_SEGMENT_SIZE)];
		m_numberOfFreeSegments = (byte) (SECLOG_SIZE / SECLOG_SEGMENT_SIZE);

		createLogAndWriteHeader(SECLOG_HEADER);
	}

	// Getter
	/**
	 * Returns the NodeID
	 * @return the NodeID
	 */
	public final short getNodeID() {
		return m_nodeID;
	}

	/**
	 * Returns the next version for ChunkID
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the next version
	 */
	public final EpochVersion getNextVersion(final long p_chunkID) {
		EpochVersion ret;

		if (m_storesMigrations) {
			ret = m_hashTable.getNext(p_chunkID);
		} else {
			ret = m_hashTable.getNext(ChunkID.getLocalID(p_chunkID));
		}

		return ret;
	}

	/**
	 * Returns the current version for ChunkID
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the current version
	 */
	public final EpochVersion getCurrentVersion(final long p_chunkID) {
		return m_hashTable.get(p_chunkID);
	}

	/**
	 * Returns the RangeID (for migrations) or first ChunkID of backup range
	 * @return the RangeID or first ChunkID
	 */
	public final long getRangeIDOrFirstLocalID() {
		return m_rangeIDOrFirstLocalID;
	}

	@Override
	public long getOccupiedSpace() {
		return m_numberOfBytes;
	}

	/**
	 * Returns the invalid counter
	 * @return the invalid counter
	 */
	public final int getLogInvalidCounter() {
		return m_numberOfInvalidsInLog;
	}

	/**
	 * Returns all segment sizes
	 * @return all segment sizes
	 */
	public final String getSegmentDistribution() {
		String ret = "++++Distribution: | ";

		for (SegmentHeader header : m_segmentHeaders) {
			if (header != null) {
				ret += header.getUsedBytes() + ", u=" + String.format("%.2f", header.getUtilization()) + " | ";
			}
		}

		return ret;
	}

	/**
	 * Returns whether this secondary log is currently accessed by reorg. thread
	 * @return whether this secondary log is currently accessed by reorg. thread
	 */
	public final boolean isAccessed() {
		return m_isAccessed;
	}

	// Setter
	/**
	 * Sets the access flag
	 * @param p_flag
	 *            the new status
	 */
	public final void setAccessFlag(final boolean p_flag) {
		m_isAccessed = p_flag;

		// Helpful for debugging, but may cause null pointer exception for writer thread
		/*-if (!p_flag) {
			m_activeSegment = null;//
		}*/
	}

	/**
	 * Invalidates a Chunk
	 * @param p_chunkID
	 *            the ChunkID
	 */
	public final void invalidateChunk(final long p_chunkID) {
		if (m_storesMigrations) {
			m_hashTable.put(p_chunkID, 0, -1);
		} else {
			m_hashTable.put(ChunkID.getLocalID(p_chunkID), 0, -1);
		}
		incLogInvalidCounter();
	}

	/**
	 * Increments invalid counter
	 */
	public final void incLogInvalidCounter() {
		m_numberOfInvalidsInLog++;
	}

	/**
	 * Resets invalid counter
	 */
	private void resetLogInvalidCounter() {
		m_numberOfInvalidsInLog = 0;
	}

	// Methods
	@Override
	public final int appendData(final byte[] p_data, final int p_offset, final int p_length) throws IOException, InterruptedException {
		int length = p_length;
		int logEntrySize;
		int rangeSize = 0;
		SegmentHeader header;
		AbstractLogEntryHeader logEntryHeader;

		if (length <= 0 || length > SECLOG_SIZE) {
			throw new IllegalArgumentException("Error: Invalid data size");
		} else {
			while (SECLOG_SIZE - m_numberOfBytes < length) {
				System.out.println("Secondary log for " + getNodeID() + " is full. Initializing reorganization and awaiting execution.");
				signalReorganizationAndWait();
			}

			/*
			 * Appending data cases:
			 * 1. This secondary log is accessed by the reorganization thread:
			 * a. No active segment or buffer too large to fit in: Create (new) "active segment" with given data
			 * b. Put data in currently active segment
			 * 2.
			 * a. Buffer is large (at least 90% of segment size): Create new segment and append it
			 * b. Fill partly used segments and put the rest (if there is data left) in a new segment and append it
			 */
			if (m_isAccessed) {
				// Reorganization thread is working on this secondary log -> only write in active segment
				if (m_activeSegment != null && length + m_activeSegment.getUsedBytes() <= SECLOG_SEGMENT_SIZE) {
					// Fill active segment
					writeToSecondaryLog(p_data, p_offset, m_activeSegment.getIndex() * SECLOG_SEGMENT_SIZE + m_activeSegment.getUsedBytes(), length, true);
					m_numberOfBytes += length;
					m_activeSegment.updateUsedBytes(length);
					length = 0;
				} else {
					if (m_activeSegment != null) {
						// There is not enough space in active segment to store the whole buffer -> first fill current one
						header = m_segmentHeaders[m_activeSegment.getIndex()];
						while (rangeSize < length) {
							logEntryHeader = AbstractLogEntryHeader.getSecondaryHeader(p_data, p_offset + rangeSize, m_storesMigrations);
							logEntrySize = logEntryHeader.getHeaderSize(p_data, p_offset + rangeSize)
									+ logEntryHeader.getLength(p_data, p_offset + rangeSize);
							if (header.getFreeBytes() - rangeSize < logEntrySize) {
								break;
							}
							rangeSize += logEntrySize;
						}
						if (rangeSize > 0) {
							writeToSecondaryLog(p_data, p_offset,
									(long) m_activeSegment.getIndex() * SECLOG_SEGMENT_SIZE + header.getUsedBytes(), rangeSize, true);
							m_numberOfBytes += rangeSize;
							header.updateUsedBytes(rangeSize);
							length -= rangeSize;
						}
					}

					// There is no active segment or the active segment is full
					length = createNewSegmentAndFill(p_data, p_offset + rangeSize, length, true);
					if (length > 0) {
						// There is no free segment -> fill partly used segments
						length = fillPartlyUsedSegments(p_data, p_offset + rangeSize, length, true);

						if (length > 0) {
							System.out.println("Error: Secondary Log full!");
						}
					}
				}
			} else {
				if (m_activeSegment != null) {
					m_activeSegment = null;
				}

				if (m_numberOfFreeSegments > 0 && length >= SECLOG_SEGMENT_SIZE * 0.9) {
					// Create new segment and fill it
					length = createNewSegmentAndFill(p_data, p_offset, length, false);
				} else {
					// Fill partly used segments if log iteration (remove task) is not in progress
					length = fillPartlyUsedSegments(p_data, p_offset, length, false);
					if (length > 0) {
						// There are still objects in buffer -> create new segment and fill it
						if (m_numberOfFreeSegments > 0) {
							length = createNewSegmentAndFill(p_data, p_offset, length, false);
						} else {
							System.out.println("Error: Secondary Log full!");
						}
					}
				}
			}
		}

		if (m_numberOfBytes >= m_secondaryLogReorgThreshold) {
			signalReorganization();
			System.out.println("Threshold breached for secondary log of " + getNodeID() + ". Initializing reorganization.");
		}

		return p_length - length;
	}

	/**
	 * Creates a new segment and fills it
	 * @param p_data
	 *            the buffer
	 * @param p_offset
	 *            the offset within the buffer
	 * @param p_length
	 *            the range length
	 * @param p_isAccessed
	 *            whether the reorganization thread is active on this log or not
	 * @return the remained length
	 * @throws IOException
	 *             if the secondary log could not be read
	 */
	public final int createNewSegmentAndFill(final byte[] p_data, final int p_offset, final int p_length, final boolean p_isAccessed) throws IOException {
		int ret = p_length;
		short segment;
		SegmentHeader header;

		if (p_isAccessed) {
			m_lock.lock();
		}

		segment = getFreeSegment();
		if (segment != -1) {
			header = new SegmentHeader(segment, p_length);
			m_segmentHeaders[segment] = header;

			if (p_isAccessed) {
				m_activeSegment = header;
				m_lock.unlock();
			}

			writeToSecondaryLog(p_data, p_offset, (long) segment * SECLOG_SEGMENT_SIZE, p_length, p_isAccessed);
			m_numberOfBytes += p_length;
			ret = 0;
		} else {
			if (p_isAccessed) {
				m_lock.unlock();
			}
		}

		return ret;
	}

	/**
	 * Fills partly used segments
	 * @param p_data
	 *            the buffer
	 * @param p_offset
	 *            the offset within the buffer
	 * @param p_length
	 *            the range length
	 * @param p_isAccessed
	 *            whether the reorganization thread is active on this log or not
	 * @return the remained length
	 * @throws IOException
	 *             if the secondary log could not be read
	 */
	public final int fillPartlyUsedSegments(final byte[] p_data, final int p_offset, final int p_length, final boolean p_isAccessed) throws IOException {
		short segment = -1;
		int offset = p_offset;
		int rangeSize;
		int logEntrySize;
		int length = p_length;
		SegmentHeader header;
		AbstractLogEntryHeader logEntryHeader;

		while (length > 0) {
			if (p_isAccessed) {
				m_lock.lock();
			}

			segment = getUsedSegment(length);
			header = m_segmentHeaders[segment];

			if (header == null) {
				break;
			}

			if (p_isAccessed) {
				// Set active segment. Must be synchronized.
				m_activeSegment = header;
				m_lock.unlock();
			}

			if (length <= header.getFreeBytes()) {
				writeToSecondaryLog(p_data, offset, (long) segment * SECLOG_SEGMENT_SIZE + header.getUsedBytes(), length, p_isAccessed);
				m_numberOfBytes += length;
				header.updateUsedBytes(length);
				length = 0;
			} else {
				rangeSize = 0;
				while (length - rangeSize > 0) {
					logEntryHeader = AbstractLogEntryHeader.getSecondaryHeader(p_data, offset + rangeSize, m_storesMigrations);
					logEntrySize = logEntryHeader.getHeaderSize(p_data, offset + rangeSize)
							+ logEntryHeader.getLength(p_data, offset + rangeSize);
					if (header.getFreeBytes() - rangeSize > logEntrySize) {
						rangeSize += logEntrySize;
					} else {
						break;
					}
				}
				if (rangeSize > 0) {
					writeToSecondaryLog(p_data, offset, (long) segment * SECLOG_SEGMENT_SIZE + header.getUsedBytes(), rangeSize, p_isAccessed);
					m_numberOfBytes += rangeSize;
					header.updateUsedBytes(rangeSize);
					length -= rangeSize;
					offset += rangeSize;
				}
			}
		}

		return length;
	}

	/**
	 * Returns the index of a free segment
	 * @return the index of a free segment
	 */
	private byte getFreeSegment() {
		byte ret = -1;
		byte i = 0;

		while (i < m_segmentHeaders.length) {
			if (m_segmentHeaders[i] == null || m_segmentHeaders[i].getFreeBytes() == SECLOG_SEGMENT_SIZE) {
				ret = i;
				break;
			}
			i++;
		}
		m_numberOfFreeSegments--;

		return ret;
	}

	/**
	 * Returns the index of the best-fitting segment
	 * @param p_length
	 *            the length of the data
	 * @return the index of the best-fitting segment
	 */
	private short getUsedSegment(final int p_length) {
		short ret = -1;
		short bestFitSegment = 0;
		short maxSegment = 0;
		short i = 0;
		int freeBytes;
		int bestFit = Integer.MAX_VALUE;
		int max = 0;

		while (i < m_segmentHeaders.length) {
			if (m_segmentHeaders[i] == null) {
				if (bestFit < p_length || bestFit == Integer.MAX_VALUE) {
					ret = i;
				}
				break;
			}
			freeBytes = m_segmentHeaders[i].getFreeBytes();
			if (freeBytes >= p_length) {
				if (freeBytes < bestFit) {
					bestFit = freeBytes;
					bestFitSegment = i;
				}
			} else if (freeBytes > max) {
				max = freeBytes;
				maxSegment = i;
			}
			i++;
		}

		if (ret == -1) {
			if (bestFit >= p_length && bestFit != Integer.MAX_VALUE) {
				ret = bestFitSegment;
			} else {
				ret = maxSegment;
			}
		}

		return ret;
	}

	/**
	 * Returns a list with all log entries wrapped in chunks
	 * @param p_doCRCCheck
	 *            whether to check the payload or not
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @return ArrayList with all log entries as chunks
	 */
	public final Chunk[] recoverAllLogEntries(final boolean p_doCRCCheck) throws IOException, InterruptedException {
		int i = 0;
		int offset = 0;
		int logEntrySize;
		int payloadSize;
		long checksum = -1;
		long chunkID;
		byte[][] segments;
		byte[] payload;
		HashMap<Long, Chunk> chunkMap = null;
		AbstractLogEntryHeader logEntryHeader;

		// TODO: Guarantee that there is no more data to come
		// TODO: lock log and reorganize during recovery
		m_activeSegment = null;
		signalReorganizationAndWait();
		try {
			chunkMap = new HashMap<Long, Chunk>();
			segments = readAllSegments();
			while (i < segments.length && segments[i] != null) {
				while (offset < segments[i].length) {
					// Determine header of next log entry
					logEntryHeader = AbstractLogEntryHeader.getSecondaryHeader(segments[i], offset, m_storesMigrations);
					if (m_storesMigrations) {
						chunkID = logEntryHeader.getCID(segments[i], offset);
					} else {
						chunkID = ((long) m_nodeID << 48) + logEntryHeader.getCID(segments[i], offset);
					}
					payloadSize = logEntryHeader.getLength(segments[i], offset);
					if (USE_CHECKSUM) {
						checksum = logEntryHeader.getChecksum(segments[i], offset);
					}
					logEntrySize = logEntryHeader.getHeaderSize(segments[i], offset) + payloadSize;

					// Read payload and create chunk
					if (offset + logEntrySize <= segments[i].length) {
						// Create chunk only if log entry complete
						payload = new byte[payloadSize];
						System.arraycopy(segments[i], offset + logEntryHeader.getHeaderSize(segments[i], offset), payload, 0, payloadSize);
						if (p_doCRCCheck) {
							if (USE_CHECKSUM && AbstractLogEntryHeader.calculateChecksumOfPayload(payload) != checksum) {
								// Ignore log entry
								offset += logEntrySize;
								continue;
							}
						}
						chunkMap.put(chunkID, new Chunk(chunkID, payload));
					}
					offset += logEntrySize;
				}
				offset = 0;
				i++;
			}
		} finally {
			segments = null;
			payload = null;
		}
		return chunkMap.values().toArray(new Chunk[chunkMap.size()]);
	}

	/**
	 * Returns a list with all log entries in file wrapped in chunks
	 * @param p_fileName
	 *            the file name of the secondary log
	 * @param p_path
	 *            the path of the directory the file is in
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @return ArrayList with all log entries as chunks
	 */
	public static Chunk[] recoverBackupRangeFromFile(final String p_fileName, final String p_path) throws IOException, InterruptedException {
		short nodeID;
		int i = 0;
		int offset = 0;
		int logEntrySize;
		int payloadSize;
		long checksum = -1;
		long chunkID;
		boolean storesMigrations;
		byte[][] segments;
		byte[] payload;
		HashMap<Long, Chunk> chunkMap = null;
		AbstractLogEntryHeader logEntryHeader;

		nodeID = Short.parseShort(p_fileName.split("_")[0].substring(1));
		storesMigrations = p_fileName.contains("M");

		try {
			chunkMap = new HashMap<Long, Chunk>();
			segments = readAllSegmentsFromFile(p_path + p_fileName);
			// TODO: Reorganize log
			while (i < segments.length && segments[i] != null) {
				while (offset < segments[i].length && segments[i][offset] != 0) {
					// Determine header of next log entry
					logEntryHeader = AbstractLogEntryHeader.getSecondaryHeader(segments[i], offset, storesMigrations);
					if (storesMigrations) {
						chunkID = logEntryHeader.getCID(segments[i], offset);
					} else {
						chunkID = ((long) nodeID << 48) + logEntryHeader.getCID(segments[i], offset);
					}
					payloadSize = logEntryHeader.getLength(segments[i], offset);
					if (USE_CHECKSUM) {
						checksum = logEntryHeader.getChecksum(segments[i], offset);
					}
					logEntrySize = logEntryHeader.getHeaderSize(segments[i], offset) + payloadSize;

					// Read payload and create chunk
					if (offset + logEntrySize <= segments[i].length) {
						// Create chunk only if log entry complete
						payload = new byte[payloadSize];
						System.arraycopy(segments[i], offset + logEntryHeader.getHeaderSize(segments[i], offset), payload, 0, payloadSize);
						if (USE_CHECKSUM && AbstractLogEntryHeader.calculateChecksumOfPayload(payload) != checksum) {
							// Ignore log entry
							offset += logEntrySize;
							continue;
						}
						chunkMap.put(chunkID, new Chunk(chunkID, payload));
					}
					offset += logEntrySize;
				}
				offset = 0;
				i++;
			}
		} finally {
			segments = null;
			payload = null;
		}

		return chunkMap.values().toArray(new Chunk[chunkMap.size()]);
	}

	/**
	 * Returns all segments of secondary log
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @return all data
	 * @note executed only by reorganization thread
	 */
	public final byte[][] readAllSegments() throws IOException, InterruptedException {
		byte[][] result = null;
		SegmentHeader header;
		int length;

		result = new byte[m_segmentHeaders.length][];
		for (int i = 0; i < m_segmentHeaders.length; i++) {
			header = m_segmentHeaders[i];
			if (header != null) {
				length = header.getUsedBytes();
				result[i] = new byte[length];
				readFromSecondaryLog(result[i], length, i * SECLOG_SEGMENT_SIZE, true);
			}
		}
		return result;
	}

	/**
	 * Returns all segments of secondary log
	 * @param p_path
	 *            the path of the file
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @return all data
	 * @note executed only by reorganization thread
	 */
	private static byte[][] readAllSegmentsFromFile(final String p_path) throws IOException, InterruptedException {
		byte[][] result = null;
		int numberOfSegments;
		RandomAccessFile randomAccessFile;

		// TODO: Where is the end of a segment?
		numberOfSegments = (int) (SECLOG_SIZE / SECLOG_SEGMENT_SIZE);
		randomAccessFile = new RandomAccessFile(new File(p_path), "r");
		result = new byte[numberOfSegments][];
		for (int i = 0; i < numberOfSegments; i++) {
			result[i] = new byte[SECLOG_SEGMENT_SIZE];
			readFromSecondaryLogFile(result[i], SECLOG_SEGMENT_SIZE, i * SECLOG_SEGMENT_SIZE, randomAccessFile, (short) SECLOG_HEADER.length);
		}
		randomAccessFile.close();

		return result;
	}

	/**
	 * Returns given segment of secondary log
	 * @param p_segment
	 *            the segment
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @return the segment's data
	 * @note executed only by reorganization thread
	 */
	private byte[] readSegment(final int p_segment) throws IOException, InterruptedException {
		byte[] result = null;
		SegmentHeader header;
		int length;

		header = m_segmentHeaders[p_segment];
		if (header != null) {
			length = header.getUsedBytes();
			result = new byte[length];
			readFromSecondaryLog(result, length, p_segment * SECLOG_SEGMENT_SIZE, true);
		}
		return result;
	}

	/**
	 * Updates log segment
	 * @param p_buffer
	 *            the buffer
	 * @param p_length
	 *            the segment length
	 * @param p_segmentIndex
	 *            the segment index
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @note executed only by reorganization thread
	 */
	private void updateSegment(final byte[] p_buffer, final int p_length, final int p_segmentIndex) throws IOException, InterruptedException {
		SegmentHeader header;

		// Mark the end of the segment
		p_buffer[p_length] = 0;

		// Overwrite segment on log
		writeToSecondaryLog(p_buffer, 0, p_segmentIndex * SECLOG_SEGMENT_SIZE, p_length + 1, true);

		// Update segment header
		header = m_segmentHeaders[p_segmentIndex];
		header.reset();
		header.updateUsedBytes(p_length);
	}

	/**
	 * Frees segment
	 * @param p_segment
	 *            the segment
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @note executed only by reorganization thread
	 */
	private void freeSegment(final int p_segment) throws IOException, InterruptedException {
		SegmentHeader header;

		writeToSecondaryLog(new byte[] {0}, 0, p_segment * SECLOG_SEGMENT_SIZE, 1, true);
		header = m_segmentHeaders[p_segment];
		header.reset();
		m_numberOfFreeSegments++;
	}

	@Override
	public String toString() {
		return "NodeID: " + getNodeID() + " - RangeID: " + getRangeIDOrFirstLocalID() + " - Written bytes: " + m_numberOfBytes;
	}

	/**
	 * Wakes up the reorganization thread
	 * @throws InterruptedException
	 *             if caller is interrupted
	 */
	private void signalReorganization() throws InterruptedException {

		try {
			m_reorganizationThread.lock();
			m_reorganizationThread.setLog(this);
			m_reorganizationThread.signal();
		} finally {
			m_reorganizationThread.unlock();
		}
	}

	/**
	 * Wakes up the reorganization thread and waits until reorganization is
	 * finished
	 * @throws InterruptedException
	 *             if caller is interrupted
	 */
	private void signalReorganizationAndWait() throws InterruptedException {

		try {
			m_reorganizationThread.lock();
			m_reorganizationThread.setLog(this);
			m_reorganizationThread.signal();
			m_reorganizationThread.await();
		} finally {
			m_reorganizationThread.unlock();
		}
	}

	/**
	 * Reorganizes all segments
	 */
	public final void reorganizeAll() {
		for (int i = 0; i < m_segmentHeaders.length; i++) {
			if (m_segmentHeaders[i] != null) {
				reorganizeSegment(i);
			}
		}
	}

	/**
	 * Reorganizes one segment by choosing the segment with best cost-benefit ratio
	 */
	public final void reorganizeIteratively() {
		reorganizeSegment(chooseSegment());
	}

	/**
	 * Reorganizes one given segment
	 * @param p_segmentIndex
	 *            the segments index
	 */
	private void reorganizeSegment(final int p_segmentIndex) {
		int length;
		int readBytes = 0;
		int writtenBytes = 0;
		// int removedObjects = 0;
		long chunkID;
		byte[] segmentData;
		byte[] newData;
		AbstractLogEntryHeader logEntryHeader;

		if (-1 != p_segmentIndex) {
			// System.out.println("\n\n\n\nStarting reorganisation!");
			try {
				segmentData = readSegment(p_segmentIndex);
				newData = new byte[SECLOG_SEGMENT_SIZE];

				// TODO: Remove all old versions in segment if a tombstone appears?
				// TODO: Remove object if there is a newer version in this segment?
				while (readBytes < segmentData.length) {
					logEntryHeader = AbstractLogEntryHeader.getSecondaryHeader(segmentData, readBytes, m_storesMigrations);
					length = logEntryHeader.getHeaderSize(segmentData, readBytes) + logEntryHeader.getLength(segmentData, readBytes);
					chunkID = logEntryHeader.getCID(segmentData, readBytes);
					// logEntryHeader.print(segmentData, readBytes);

					if (m_hashTable.get(chunkID).equals(logEntryHeader.getVersion(segmentData, readBytes))) {
						System.arraycopy(segmentData, readBytes, newData, writtenBytes, length);
						writtenBytes += length;
					} else {
						// removedObjects++;
					}
					readBytes += length;
				}
				m_lock.lock();
				if (m_activeSegment == null || m_activeSegment.getIndex() != p_segmentIndex) {
					if (writtenBytes < readBytes) {
						if (writtenBytes > 0) {
							updateSegment(newData, writtenBytes, p_segmentIndex);
						} else {
							freeSegment(p_segmentIndex);
						}
						m_numberOfBytes -= readBytes - writtenBytes;
					}
					/*-} else {
						removedObjects = 0;
						removedTombstones = 0;*/
				}
				m_lock.unlock();
			} catch (final IOException | InterruptedException e) {
				System.out.println("Reorganization failed!");
			}

			/*-if (removedObjects != 0) {
				System.out.println("\n- Reorganization of Segment: " + p_segmentIndex
						+ "(Peer: " + m_nodeID + ", Range: " + getRangeIDOrFirstLocalID() + ") finished:");
				System.out.println("-- " + removedObjects + " entries removed\n");
			}*/
		}
	}

	/**
	 * Determines the next segment to reorganize
	 * @return the chosen segment
	 */
	private int chooseSegment() {
		int ret = -1;
		long costBenefitRatio;
		long max = -1;
		SegmentHeader currentSegment;

		// Cost-benefit ratio: ((1-u)*age)/(1+u)
		for (int i = 0; i < m_segmentHeaders.length; i++) {
			currentSegment = m_segmentHeaders[i];
			if (currentSegment != null) {
				costBenefitRatio = (long) ((1 - currentSegment.getUtilization()) * currentSegment.getAge() / (1 + currentSegment.getUtilization()));

				if (costBenefitRatio > max) {
					max = costBenefitRatio;
					ret = i;
				}
			}
		}

		return ret;
	}

	/**
	 * Gets current versions from log
	 * @param p_hashtable
	 *            a hash table to store version numbers in
	 */
	public final void getCurrentVersions(final VersionsHashTable p_hashtable) {

	}

	// Classes
	/**
	 * SegmentHeader
	 * @author Kevin Beineke 07.11.2014
	 */
	private final class SegmentHeader {

		// Attributes
		private int m_index;
		private int m_usedBytes;
		private int m_deletedBytes;
		private long m_lastAccess;

		// Constructors
		/**
		 * Creates an instance of SegmentHeader
		 * @param p_usedBytes
		 *            the number of used bytes
		 * @param p_index
		 *            the index within the log
		 */
		private SegmentHeader(final int p_index, final int p_usedBytes) {
			m_index = p_index;
			m_usedBytes = p_usedBytes;
			m_deletedBytes = 0;
			m_lastAccess = System.currentTimeMillis();
		}

		// Getter
		/**
		 * Returns the utilization
		 * @return the utilization
		 */
		private float getUtilization() {
			float ret = 1;

			if (m_usedBytes > 0) {
				ret = 1 - (float) m_deletedBytes / m_usedBytes;
			}

			return ret;
		}

		/**
		 * Returns the index
		 * @return the index
		 */
		private int getIndex() {
			return m_index;
		}

		/**
		 * Returns number of used bytes
		 * @return number of used bytes
		 */
		private int getUsedBytes() {
			return m_usedBytes;
		}

		/**
		 * Returns number of used bytes
		 * @return number of used bytes
		 */
		private int getFreeBytes() {
			return SECLOG_SEGMENT_SIZE - m_usedBytes;
		}

		/**
		 * Returns the age of this segment
		 * @return the age of this segment
		 */
		private long getAge() {
			return System.currentTimeMillis() - m_lastAccess;
		}

		// Setter
		/**
		 * Updates the number of used bytes
		 * @param p_writtenBytes
		 *            the number of written bytes
		 */
		private void updateUsedBytes(final int p_writtenBytes) {
			m_usedBytes += p_writtenBytes;
			m_lastAccess = System.currentTimeMillis();
		}

		/**
		 * Updates the number of deleted bytes
		 * @param p_deletedBytes
		 *            the number of deleted bytes
		 */
		private void updateDeletedBytes(final int p_deletedBytes) {
			m_deletedBytes += p_deletedBytes;
			m_lastAccess = System.currentTimeMillis();
		}

		/**
		 * Resets the segment header
		 */
		private void reset() {
			m_deletedBytes = 0;
			m_usedBytes = 0;
			m_lastAccess = System.currentTimeMillis();
		}
	}

}
