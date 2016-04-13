
package de.hhu.bsinfo.dxram.log.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.log.LogService;
import de.hhu.bsinfo.dxram.log.SecondaryLogsReorgThread;
import de.hhu.bsinfo.dxram.log.header.AbstractLogEntryHeader;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.utils.Tools;

/**
 * This class implements the secondary log
 * @author Kevin Beineke 23.10.2014
 */
public class SecondaryLog extends AbstractLog {

	// Constants
	private static final String SECLOG_PREFIX_FILENAME = "sec";
	private static final String SECLOG_POSTFIX_FILENAME = ".log";
	private static final byte[] SECLOG_HEADER = "DXRAMSecLogv1".getBytes(Charset.forName("UTF-8"));

	private static final int VERSIONS_BUFFER_CAPACITY = 65536;

	// Attributes
	private LoggerComponent m_logger;

	private short m_nodeID;
	private long m_rangeIDOrFirstLocalID;
	private long m_secondaryLogSize;
	private int m_logSegmentSize;
	private boolean m_useChecksum;

	private VersionsBuffer m_versionsBuffer;

	private long m_secondaryLogReorgThreshold;
	private SecondaryLogsReorgThread m_reorganizationThread;

	private SegmentHeader[] m_segmentHeaders;

	private boolean m_isAccessed;
	private SegmentHeader m_activeSegment;
	private SegmentHeader m_reorgSegment;
	private ReentrantLock m_segmentAssignmentlock;

	private byte[] m_reorgVector;
	private int m_segmentReorgCounter;

	private boolean m_storesMigrations;

	// Constructors
	/**
	 * Creates an instance of SecondaryLog with default configuration except
	 * secondary log size
	 * @param p_logService
	 *            the log service to enable calling access granting methods in VersionsBuffer
	 * @param p_logger
	 *            the logger component
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
	 * @param p_backupDirectory
	 *            the backup directory
	 * @param p_secondaryLogSize
	 *            the size of a secondary log
	 * @param p_flashPageSize
	 *            the flash page size
	 * @param p_logSegmentSize
	 *            the segment size
	 * @param p_reorgUtilizationThreshold
	 *            the threshold size for a secondary size to trigger reorganization
	 * @param p_useChecksum
	 *            the logger component
	 * @throws IOException
	 *             if secondary log could not be created
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 */
	public SecondaryLog(final LogService p_logService, final LoggerComponent p_logger, final SecondaryLogsReorgThread p_reorganizationThread,
			final short p_nodeID, final long p_rangeIDOrFirstLocalID, final String p_rangeIdentification, final boolean p_storesMigrations,
			final String p_backupDirectory, final long p_secondaryLogSize, final int p_flashPageSize, final int p_logSegmentSize,
			final int p_reorgUtilizationThreshold, final boolean p_useChecksum) throws IOException,
			InterruptedException {
		super(new File(p_backupDirectory + "N" + p_nodeID + "_" + SECLOG_PREFIX_FILENAME + p_nodeID + "_" + p_rangeIdentification
				+ SECLOG_POSTFIX_FILENAME), p_secondaryLogSize, SECLOG_HEADER.length);
		if (p_secondaryLogSize < p_flashPageSize) {
			throw new IllegalArgumentException("Error: Secondary log too small");
		}
		m_logger = p_logger;
		m_secondaryLogSize = p_secondaryLogSize;
		m_logSegmentSize = p_logSegmentSize;
		m_useChecksum = p_useChecksum;

		m_storesMigrations = p_storesMigrations;
		m_segmentAssignmentlock = new ReentrantLock(false);

		m_nodeID = p_nodeID;
		m_rangeIDOrFirstLocalID = p_rangeIDOrFirstLocalID;

		m_versionsBuffer = new VersionsBuffer(p_logService, m_logger, VERSIONS_BUFFER_CAPACITY * 4, 0.9f, p_backupDirectory + "N" + p_nodeID
				+ "_" + SECLOG_PREFIX_FILENAME + p_nodeID + "_" + p_rangeIdentification + ".ver");

		m_reorganizationThread = p_reorganizationThread;

		m_secondaryLogReorgThreshold = (int) (p_secondaryLogSize * ((double) p_reorgUtilizationThreshold / 100));
		m_segmentReorgCounter = 0;
		m_segmentHeaders = new SegmentHeader[(int) (p_secondaryLogSize / p_logSegmentSize)];
		m_reorgVector = new byte[(int) (p_secondaryLogSize / p_logSegmentSize)];

		createLogAndWriteHeader(SECLOG_HEADER);

		m_logger.trace(getClass(), "Initialized secondary log (" + m_secondaryLogSize + ")");
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
	public final Version getNextVersion(final long p_chunkID) {
		Version ret;

		if (m_storesMigrations) {
			ret = m_versionsBuffer.getNext(p_chunkID);
		} else {
			ret = m_versionsBuffer.getNext(ChunkID.getLocalID(p_chunkID));
		}

		return ret;
	}

	/**
	 * Returns the current version for ChunkID
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the current version
	 */
	public final Version getCurrentVersion(final long p_chunkID) {
		return m_versionsBuffer.get(p_chunkID);
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
		return determineLogSize();
	}

	/**
	 * Returns true if there are segments that were not yet reorganized in this eon and the eon has exceeded half time
	 * @return whether this log needs to be reorganized or not
	 */
	public final boolean needToBeReorganized() {
		boolean ret = false;

		if (m_versionsBuffer.getEpoch() > Math.pow(2, 14)) {
			for (SegmentHeader segmentHeader : m_segmentHeaders) {
				if (segmentHeader != null && segmentHeader.getUsedBytes() != 0 && !segmentHeader.wasReorganized()) {
					ret = true;
					break;
				}
			}
		}

		return ret;
	}

	/**
	 * Returns all segment sizes
	 * @return all segment sizes
	 */
	public final String getSegmentDistribution() {
		String ret = "++++Distribution: | ";
		SegmentHeader header;

		for (int i = 0; i < m_segmentHeaders.length; i++) {
			header = m_segmentHeaders[i];
			if (header != null) {
				ret += i + " " + header.getUsedBytes() + ", u=" + String.format("%.2f", header.getUtilization()) + " | ";
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
			m_versionsBuffer.put(p_chunkID, -1);
		} else {
			m_versionsBuffer.put(ChunkID.getLocalID(p_chunkID), -1);
		}
	}

	// Methods
	/**
	 * Returns the sum of all segment sizes
	 * @return the sum of all segment sizes
	 */
	private int determineLogSize() {
		int ret = 0;

		for (int i = 0; i < m_segmentHeaders.length; i++) {
			if (m_segmentHeaders[i] != null) {
				ret += m_segmentHeaders[i].getUsedBytes();
			}
		}

		return ret;
	}

	@Override
	public final int appendData(final byte[] p_data, final int p_offset, final int p_length) throws IOException, InterruptedException {
		int length = p_length;
		int logEntrySize;
		int rangeSize = 0;
		boolean isSignaled = false;
		SegmentHeader header;
		AbstractLogEntryHeader logEntryHeader;

		if (length <= 0 || length > m_secondaryLogSize) {
			throw new IllegalArgumentException("Error: Invalid data size (" + length + ")");
		} else {
			while (m_secondaryLogSize - determineLogSize() < length) {
				m_logger.warn(SecondaryLog.class, "Secondary log for " + getNodeID() + " is full. Initializing reorganization and awaiting execution.");
				signalReorganizationAndWait();
			}

			// Change epoch
			if (m_versionsBuffer.size() >= VERSIONS_BUFFER_CAPACITY * 0.65) {
				if (!m_isAccessed) {
					// Write versions buffer to SSD
					if (m_versionsBuffer.flush()) {
						for (SegmentHeader segmentHeader : m_segmentHeaders) {
							if (segmentHeader != null) {
								segmentHeader.beginEon();
							}
						}
					}
				} else {
					// Force reorganization thread to flush all versions (even though it is reorganizing this log
					// currently -> high update rate)
					signalReorganization();
					isSignaled = true;
				}
			}

			/*
			 * Appending data cases:
			 * 1. This secondary log is accessed by the reorganization thread:
			 * a. Put data in currently active segment
			 * b. No active segment or buffer too large to fit in: Create (new) "active segment" with given data
			 * 2.
			 * a. Buffer is large (at least 90% of segment size): Create new segment and append it
			 * b. Fill partly used segments and put the rest (if there is data left) in a new segment and append it
			 */
			if (m_isAccessed) {
				// Reorganization thread is working on this secondary log -> only write in active segment
				if (m_activeSegment != null && m_activeSegment.getFreeBytes() >= length) {
					// Fill active segment
					writeToSecondaryLog(p_data, p_offset, (long) m_activeSegment.getIndex() * m_logSegmentSize + m_activeSegment.getUsedBytes(),
							length, true);
					m_activeSegment.updateUsedBytes(length);
					length = 0;
				} else {
					if (m_activeSegment != null) {
						// There is not enough space in active segment to store the whole buffer -> first fill current
						// one
						header = m_segmentHeaders[m_activeSegment.getIndex()];
						while (true) {
							logEntryHeader = AbstractLogEntryHeader.getSecondaryHeader(p_data, p_offset + rangeSize, m_storesMigrations);
							logEntrySize = logEntryHeader.getHeaderSize(p_data, p_offset + rangeSize)
									+ logEntryHeader.getLength(p_data, p_offset + rangeSize);
							if (logEntrySize > header.getFreeBytes() - rangeSize) {
								break;
							} else {
								rangeSize += logEntrySize;
							}
						}
						if (rangeSize > 0) {
							writeToSecondaryLog(p_data, p_offset, (long) header.getIndex() * m_logSegmentSize + header.getUsedBytes(), rangeSize, true);
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
							m_logger.error(SecondaryLog.class, "Secondary log is full!");
						}
					}
				}
			} else {
				if (m_activeSegment != null) {
					m_activeSegment = null;
				}

				if (length >= m_logSegmentSize * 0.9) {
					// Create new segment and fill it
					length = createNewSegmentAndFill(p_data, p_offset, length, false);
				}
				if (length > 0) {
					// Fill partly used segments if log iteration (remove task) is not in progress
					length = fillPartlyUsedSegments(p_data, p_offset, length, false);

					if (length > 0) {
						m_logger.error(SecondaryLog.class, "Secondary log is full!");
					}
				}
			}
		}

		if (determineLogSize() >= m_secondaryLogReorgThreshold && !isSignaled) {
			signalReorganization();
			m_logger.info(SecondaryLog.class, "Threshold breached for secondary log of " + getNodeID() + ". Initializing reorganization.");
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
			m_segmentAssignmentlock.lock();
		}

		segment = getFreeSegment();
		if (segment != -1) {
			header = new SegmentHeader(segment, p_length);
			m_segmentHeaders[segment] = header;

			if (p_isAccessed) {
				// Set active segment. Must be synchronized.
				m_activeSegment = header;
				m_segmentAssignmentlock.unlock();
			}

			writeToSecondaryLog(p_data, p_offset, (long) segment * m_logSegmentSize, p_length, p_isAccessed);

			ret = 0;
		} else {
			if (p_isAccessed) {
				m_segmentAssignmentlock.unlock();
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
				m_segmentAssignmentlock.lock();
			}

			// Get the smallest used segment that has enough free space to store everything.
			// If there is no best fitting segment, choose an empty one.
			// If there is no empty one, return the segment with most free space.
			segment = getUsedSegment(length);
			header = m_segmentHeaders[segment];

			if (header == null) {
				// This segment is empty (there was no best fitting segment)
				header = new SegmentHeader(segment, length);
				m_segmentHeaders[segment] = header;

				if (p_isAccessed) {
					// Set active segment. Must be synchronized.
					m_activeSegment = header;
					m_segmentAssignmentlock.unlock();
				}

				writeToSecondaryLog(p_data, offset, (long) segment * m_logSegmentSize, length, p_isAccessed);
				length = 0;

				break;
			} else {
				if (p_isAccessed) {
					// Set active segment. Must be synchronized.
					m_activeSegment = header;
					m_segmentAssignmentlock.unlock();
				}

				if (length <= header.getFreeBytes()) {
					// All data fits in this segment
					writeToSecondaryLog(p_data, offset, (long) segment * m_logSegmentSize + header.getUsedBytes(), length, p_isAccessed);
					header.updateUsedBytes(length);
					length = 0;

					break;
				} else {
					// This is the largest left segment -> write as long as there is space left
					rangeSize = 0;
					while (true) {
						logEntryHeader = AbstractLogEntryHeader.getSecondaryHeader(p_data, offset + rangeSize, m_storesMigrations);
						logEntrySize = logEntryHeader.getHeaderSize(p_data, offset + rangeSize)
								+ logEntryHeader.getLength(p_data, offset + rangeSize);
						if (logEntrySize > header.getFreeBytes() - rangeSize) {
							break;
						} else {
							rangeSize += logEntrySize;
						}
					}
					if (rangeSize > 0) {
						writeToSecondaryLog(p_data, offset, (long) segment * m_logSegmentSize + header.getUsedBytes(), rangeSize, p_isAccessed);
						header.updateUsedBytes(rangeSize);
						length -= rangeSize;
						offset += rangeSize;
					}
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
			// Empty segment headers are null
			if (m_segmentHeaders[i] == null) {
				// Avoid reorganization segment
				if (m_reorgSegment == null || i != m_reorgSegment.getIndex()) {
					ret = i;
					break;
				}
			}
			i++;
		}

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
		short bestFitSegment = -1;
		short maxSegment = -1;
		short emptySegment = -1;
		int bestFit = Integer.MAX_VALUE;
		int max = 0;
		int freeBytes;

		for (short i = 0; i < m_segmentHeaders.length; i++) {
			if (m_segmentHeaders[i] == null) {
				// This is an empty segment. We need it if there is no best fit.
				if (emptySegment == -1) {
					emptySegment = i;
				}
			} else {
				// Avoid reorganization segment
				if (m_segmentHeaders[i] != m_reorgSegment) {
					freeBytes = m_segmentHeaders[i].getFreeBytes();
					if (freeBytes >= p_length) {
						if (freeBytes < bestFit) {
							// In current segment is more space than needed but less than in every segment before ->
							// current best fit
							bestFit = freeBytes;
							bestFitSegment = i;
						}
					} else if (freeBytes > max) {
						// In current segment is less space than needed but more than in every segment before -> current
						// maximum
						max = freeBytes;
						maxSegment = i;
					}
				}
			}
		}

		// Choose segment with following order: 1. best fit 2. empty segment 3. max space
		if (bestFitSegment != -1) {
			ret = bestFitSegment;
		} else if (emptySegment != -1) {
			ret = emptySegment;
		} else {
			ret = maxSegment;
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
			while (i < segments.length) {
				if (segments[i] != null) {
					while (offset < segments[i].length) {
						// Determine header of next log entry
						logEntryHeader = AbstractLogEntryHeader.getSecondaryHeader(segments[i], offset, m_storesMigrations);
						if (m_storesMigrations) {
							chunkID = logEntryHeader.getCID(segments[i], offset);
						} else {
							chunkID = ((long) m_nodeID << 48) + logEntryHeader.getCID(segments[i], offset);
						}
						payloadSize = logEntryHeader.getLength(segments[i], offset);
						if (m_useChecksum) {
							checksum = logEntryHeader.getChecksum(segments[i], offset);
						}
						logEntrySize = logEntryHeader.getHeaderSize(segments[i], offset) + payloadSize;

						// Read payload and create chunk
						if (offset + logEntrySize <= segments[i].length) {
							// Create chunk only if log entry complete
							payload = new byte[payloadSize];
							System.arraycopy(segments[i], offset + logEntryHeader.getHeaderSize(segments[i], offset), payload, 0, payloadSize);
							if (p_doCRCCheck) {
								if (m_useChecksum && AbstractLogEntryHeader.calculateChecksumOfPayload(payload) != checksum) {
									// Ignore log entry
									offset += logEntrySize;
									continue;
								}
							}
							chunkMap.put(chunkID, new Chunk(chunkID, ByteBuffer.wrap(payload)));
						}
						offset += logEntrySize;
					}
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
	 * @param p_useChecksum
	 *            whether checksums are used
	 * @param p_secondaryLogSize
	 *            the secondary log size
	 * @param p_logSegmentSize
	 *            the segment size
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @return ArrayList with all log entries as chunks
	 */
	public static Chunk[] recoverBackupRangeFromFile(final String p_fileName, final String p_path,
			final boolean p_useChecksum, final long p_secondaryLogSize, final int p_logSegmentSize) throws IOException, InterruptedException {
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
			segments = readAllSegmentsFromFile(p_path + p_fileName, p_secondaryLogSize, p_logSegmentSize);
			// TODO: Reorganize log
			while (i < segments.length) {
				if (segments[i] != null) {
					while (offset < segments[i].length && segments[i][offset] != 0) {
						// Determine header of next log entry
						logEntryHeader = AbstractLogEntryHeader.getSecondaryHeader(segments[i], offset, storesMigrations);
						if (storesMigrations) {
							chunkID = logEntryHeader.getCID(segments[i], offset);
						} else {
							chunkID = ((long) nodeID << 48) + logEntryHeader.getCID(segments[i], offset);
						}
						payloadSize = logEntryHeader.getLength(segments[i], offset);
						if (p_useChecksum) {
							checksum = logEntryHeader.getChecksum(segments[i], offset);
						}
						logEntrySize = logEntryHeader.getHeaderSize(segments[i], offset) + payloadSize;

						// Read payload and create chunk
						if (offset + logEntrySize <= segments[i].length) {
							// Create chunk only if log entry complete
							payload = new byte[payloadSize];
							System.arraycopy(segments[i], offset + logEntryHeader.getHeaderSize(segments[i], offset), payload, 0, payloadSize);
							if (p_useChecksum && AbstractLogEntryHeader.calculateChecksumOfPayload(payload) != checksum) {
								// Ignore log entry
								offset += logEntrySize;
								continue;
							}
							chunkMap.put(chunkID, new Chunk(chunkID, ByteBuffer.wrap(payload)));
						}
						offset += logEntrySize;
					}
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
				readFromSecondaryLog(result[i], length, i * m_logSegmentSize, true);
			}
		}
		return result;
	}

	/**
	 * Returns all segments of secondary log
	 * @param p_path
	 *            the path of the file
	 * @param p_secondaryLogSize
	 *            the secondary log size
	 * @param p_logSegmentSize
	 *            the segment size
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @return all data
	 * @note executed only by reorganization thread
	 */
	private static byte[][] readAllSegmentsFromFile(final String p_path, final long p_secondaryLogSize, final int p_logSegmentSize) throws IOException,
	InterruptedException {
		byte[][] result = null;
		int numberOfSegments;
		RandomAccessFile randomAccessFile;

		// TODO: Where is the end of a segment?
		numberOfSegments = (int) (p_secondaryLogSize / p_logSegmentSize);
		randomAccessFile = new RandomAccessFile(new File(p_path), "r");
		result = new byte[numberOfSegments][];
		for (int i = 0; i < numberOfSegments; i++) {
			result[i] = new byte[p_logSegmentSize];
			readFromSecondaryLogFile(result[i], p_logSegmentSize, i * p_logSegmentSize, randomAccessFile, (short) SECLOG_HEADER.length);
		}
		randomAccessFile.close();

		return result;
	}

	/**
	 * Returns given segment of secondary log
	 * @param p_data
	 *            the buffer to read data into
	 * @param p_segmentIndex
	 *            the segment
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @return the segment's data
	 * @note executed only by reorganization thread
	 */
	private int readSegment(final byte[] p_data, final int p_segmentIndex) throws IOException, InterruptedException {
		int ret = 0;
		SegmentHeader header;

		header = m_segmentHeaders[p_segmentIndex];
		if (header != null) {
			ret = header.getUsedBytes();
			readFromSecondaryLog(p_data, ret, p_segmentIndex * m_logSegmentSize, true);
		}
		return ret;
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

		// Mark the end of the segment (a log entry header cannot start with a zero)
		p_buffer[p_length] = 0;

		// Overwrite segment on log
		writeToSecondaryLog(p_buffer, 0, (long) p_segmentIndex * m_logSegmentSize, p_length + 1, true);

		// Update segment header
		header = m_segmentHeaders[p_segmentIndex];
		header.reset();
		header.updateUsedBytes(p_length);
		header.markSegmentAsReorganized();
	}

	/**
	 * Frees segment
	 * @param p_segmentIndex
	 *            the segment
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @note executed only by reorganization thread
	 */
	private void freeSegment(final int p_segmentIndex) throws IOException, InterruptedException {

		// Mark the end of the segment (a log entry header cannot start with a zero)
		writeToSecondaryLog(new byte[] {0}, 0, (long) p_segmentIndex * m_logSegmentSize, 1, true);
		m_segmentHeaders[p_segmentIndex] = null;
	}

	@Override
	public String toString() {
		return "NodeID: " + getNodeID() + " - RangeID: " + getRangeIDOrFirstLocalID() + " - Written bytes: " + determineLogSize();
	}

	/**
	 * Wakes up the reorganization thread
	 * @throws InterruptedException
	 *             if caller is interrupted
	 */
	private void signalReorganization() throws InterruptedException {
		m_reorganizationThread.setLogToReorgImmediately(this, false);
	}

	/**
	 * Wakes up the reorganization thread and waits until reorganization is
	 * finished
	 * @throws InterruptedException
	 *             if caller is interrupted
	 */
	private void signalReorganizationAndWait() throws InterruptedException {
		m_reorganizationThread.setLogToReorgImmediately(this, true);
	}

	/**
	 * Reorganizes all segments
	 * @param p_segmentData
	 *            a buffer to be filled with segment data (avoiding lots of allocations)
	 * @param p_allVersions
	 *            a hash table with all versions for this secondary log
	 */
	public final void reorganizeAll(final byte[] p_segmentData, final VersionsHashTable p_allVersions) {
		for (int i = 0; i < m_segmentHeaders.length; i++) {
			if (m_segmentHeaders[i] != null) {
				reorganizeSegment(i, p_segmentData, p_allVersions);
			}
		}
	}

	/**
	 * Reorganizes one segment by choosing the segment with best cost-benefit ratio
	 * @param p_segmentData
	 *            a buffer to be filled with segment data (avoiding lots of allocations)
	 * @param p_allVersions
	 *            a hash table with all versions for this secondary log
	 */
	public final void reorganizeIteratively(final byte[] p_segmentData, final VersionsHashTable p_allVersions) {
		reorganizeSegment(chooseSegment(), p_segmentData, p_allVersions);
	}

	/**
	 * Reorganizes one given segment
	 * @param p_segmentIndex
	 *            the segments index
	 * @param p_segmentData
	 *            a buffer to be filled with segment data (avoiding lots of allocations)
	 * @param p_allVersions
	 *            a hash table with all versions for this secondary log
	 */
	private void reorganizeSegment(final int p_segmentIndex, final byte[] p_segmentData, final VersionsHashTable p_allVersions) {
		int length;
		int readBytes = 0;
		int writtenBytes = 0;
		int segmentLength;
		long chunkID;
		byte[] newData;
		Version currentVersion;
		Version entryVersion;
		AbstractLogEntryHeader logEntryHeader;

		if (-1 != p_segmentIndex && p_allVersions != null) {
			m_segmentAssignmentlock.lock();
			if (m_activeSegment == null || m_activeSegment.getIndex() != p_segmentIndex) {
				m_reorgSegment = m_segmentHeaders[p_segmentIndex];
				m_segmentAssignmentlock.unlock();
				try {
					segmentLength = readSegment(p_segmentData, p_segmentIndex);
					if (segmentLength > 0) {
						newData = new byte[m_logSegmentSize];

						while (readBytes < segmentLength) {
							logEntryHeader = AbstractLogEntryHeader.getSecondaryHeader(p_segmentData, readBytes, m_storesMigrations);
							length = logEntryHeader.getHeaderSize(p_segmentData, readBytes) + logEntryHeader.getLength(p_segmentData, readBytes);
							chunkID = logEntryHeader.getCID(p_segmentData, readBytes);
							entryVersion = logEntryHeader.getVersion(p_segmentData, readBytes);

							// Get current version
							currentVersion = p_allVersions.get(chunkID);
							if (currentVersion == null || (short) (currentVersion.getEpoch() + 1) == entryVersion.getEpoch()) {
								// There is no entry in hashtable or element is more current -> get latest version from cache
								// (Epoch can only be 1 greater because there is no flushing during reorganization)
								currentVersion = m_versionsBuffer.get(chunkID);
							}

							// Compare current version with element
							if (currentVersion.equals(logEntryHeader.getVersion(p_segmentData, readBytes))) {
								System.arraycopy(p_segmentData, readBytes, newData, writtenBytes, length);
								writtenBytes += length;

								if ((currentVersion.getEpoch() & 0x8000) != m_versionsBuffer.getEon()) {
									// Update eon in both versions
									AbstractLogEntryHeader.flipEon(logEntryHeader, newData, writtenBytes - length);

									if (m_storesMigrations) {
										m_versionsBuffer.put(chunkID, currentVersion.getEpoch() ^ 1 << 15,
												currentVersion.getVersion());
									} else {
										m_versionsBuffer.put(ChunkID.getLocalID(chunkID), currentVersion.getEpoch() ^ 1 << 15,
												currentVersion.getVersion());
									}
								}
							} else {
								// Version, epoch and/or eon is different -> remove entry
							}
							readBytes += length;
						}
						if (writtenBytes < readBytes) {
							if (writtenBytes > 0) {
								updateSegment(newData, writtenBytes, p_segmentIndex);
							} else {
								freeSegment(p_segmentIndex);
							}
						}
					}
				} catch (final IOException | InterruptedException e) {
					m_logger.error(SecondaryLog.class, "Reorganization failed(" + m_rangeIDOrFirstLocalID + "): " + e);
				}
			} else {
				m_segmentAssignmentlock.unlock();
			}

			if (readBytes - writtenBytes > 0) {
				m_logger.info(getClass(), "Freed " + (readBytes - writtenBytes) + " bytes during reorganization of:"
						+ p_segmentIndex + "  " + m_nodeID + "," + m_rangeIDOrFirstLocalID + "\t " + determineLogSize() / 1024 / 1024);
			}
		}
	}

	/**
	 * Determines the next segment to reorganize
	 * @return the chosen segment
	 */
	private int chooseSegment() {
		int ret = -1;
		int tries;
		long costBenefitRatio;
		long max = -1;
		SegmentHeader currentSegment;

		/*
		 * Choose a segment regarding the cost-benefit formula (the utilization does not contain an invalid counter)
		 * Every tenth segment is chosen randomly out of all segments that have not been reorganized in this eon.
		 * Avoid segments that already have been reorganized within this epoch (-> m_reorgVector).
		 */
		if (m_segmentReorgCounter++ == 10) {
			tries = (int) (m_secondaryLogSize / m_logSegmentSize * 2);
			while (true) {
				ret = Tools.getRandomValue((int) (m_secondaryLogSize / m_logSegmentSize) - 1);
				if (m_segmentHeaders[ret] != null && !m_segmentHeaders[ret].wasReorganized() && m_reorgVector[ret] == 0 || --tries == 0) {
					break;
				}
			}
			m_segmentReorgCounter = 0;
		}

		if (ret == -1 || m_segmentHeaders[ret] == null) {
			// Original cost-benefit ratio: ((1-u)*age)/(1+u)
			for (int i = 0; i < m_segmentHeaders.length; i++) {
				currentSegment = m_segmentHeaders[i];
				if (currentSegment != null && m_reorgVector[i] == 0) {
					costBenefitRatio = currentSegment.getAge();
					if (costBenefitRatio > max) {
						max = costBenefitRatio;
						ret = i;
					}
				}
			}
		}

		if (ret != -1) {
			// Mark segment as being reorganized in this epoch
			m_reorgVector[ret] = 1;
		}

		return ret;
	}

	/**
	 * Gets current versions from log
	 */
	public final void resetReorgSegment() {
		m_reorgSegment = null;
	}

	/**
	 * Gets current versions from log
	 * @param p_allVersions
	 *            a hash table to store version numbers in
	 */
	public final void getCurrentVersions(final VersionsHashTable p_allVersions) {
		Arrays.fill(m_reorgVector, (byte) 0);

		// Read versions from SSD and write back current view
		m_versionsBuffer.readAll(p_allVersions);
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
		private long m_lastAccess;
		private boolean m_reorgInCurrEon;

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
			m_lastAccess = System.currentTimeMillis();
			m_reorgInCurrEon = true;
		}

		// Getter
		/**
		 * Returns the utilization
		 * @return the utilization
		 */
		private float getUtilization() {
			return 1 - (float) m_usedBytes / m_logSegmentSize;
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
			return m_logSegmentSize - m_usedBytes;
		}

		/**
		 * Returns the age of this segment
		 * @return the age of this segment
		 */
		private long getAge() {
			return System.currentTimeMillis() - m_lastAccess;
		}

		/**
		 * Returns whether this segment was reorganized in current eon
		 * @return whether this segment was reorganized in current eon
		 */
		private boolean wasReorganized() {
			return m_reorgInCurrEon;
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
		 * Sets the reorganization status for current eon
		 */
		private void markSegmentAsReorganized() {
			m_reorgInCurrEon = true;
		}

		/**
		 * Resets the reorganization status for new eon
		 */
		private void beginEon() {
			m_reorgInCurrEon = false;
		}

		/**
		 * Resets the segment header
		 */
		private void reset() {
			m_usedBytes = 0;
			m_lastAccess = System.currentTimeMillis();
		}
	}

}
