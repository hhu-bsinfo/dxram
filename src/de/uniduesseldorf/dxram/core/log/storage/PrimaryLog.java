
package de.uniduesseldorf.dxram.core.log.storage;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import de.uniduesseldorf.dxram.core.CoreComponentFactory;
import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.log.LogHandler;
import de.uniduesseldorf.dxram.core.log.LogInterface;
import de.uniduesseldorf.dxram.core.log.header.AbstractLogEntryHeader;
import de.uniduesseldorf.dxram.core.log.header.DefaultSecLogEntryHeader;
import de.uniduesseldorf.dxram.core.log.header.LogEntryHeaderInterface;
import de.uniduesseldorf.dxram.core.log.header.MigrationPrimLogEntryHeader;
import de.uniduesseldorf.dxram.core.log.header.MigrationPrimLogTombstone;

/**
 * This class implements the primary log. Furthermore this class manages all
 * secondary logs
 * @author Kevin Beineke 13.06.2014
 */
public final class PrimaryLog extends AbstractLog implements LogStorageInterface {

	// Constants
	private static final LogEntryHeaderInterface DEFAULT_SEC_LOG_ENTRY_HEADER = new DefaultSecLogEntryHeader();

	// Attributes
	private LogInterface m_logHandler;

	private long m_totalUsableSpace;

	// Constructors
	/**
	 * Creates an instance of PrimaryLog with user specific configuration
	 * @param p_primaryLogSize
	 *            size of the primary log
	 * @throws IOException
	 *             if primary log could not be created
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 */
	public PrimaryLog(final long p_primaryLogSize) throws IOException, InterruptedException {
		super(new File(LogHandler.BACKUP_DIRECTORY + "N" + NodeID.getLocalNodeID() + "_" + LogHandler.PRIMLOG_FILENAME), p_primaryLogSize,
				LogHandler.PRIMLOG_MAGIC_HEADER_SIZE);

		try {
			m_logHandler = CoreComponentFactory.getLogInterface();
		} catch (final DXRAMException e) {
			System.out.println("Could not get log interface");
		}

		if (p_primaryLogSize < LogHandler.PRIMLOG_MIN_SIZE) {
			throw new IllegalArgumentException("Error: Primary log too small");
		}

		m_totalUsableSpace = super.getTotalUsableSpace();

		createLogAndWriteHeader();
	}

	// Methods
	@Override
	public void closeLog() throws InterruptedException, IOException {

		super.closeRing();
	}

	@SuppressWarnings("unchecked")
	@Override
	public int appendData(final byte[] p_data, final int p_offset, final int p_length, final Object p_lengthByBackupRange) throws IOException,
			InterruptedException {
		int ret = 0;

		if (p_length <= 0 || p_length > m_totalUsableSpace) {
			throw new IllegalArgumentException("invalid data size");
		} else {
			ret = bufferAndStoreSegmentsHashSort(p_data, p_offset, p_length, (Set<Entry<Long, Integer>>) p_lengthByBackupRange);
		}
		return ret;
	}

	/**
	 * Writes given data to secondary log buffers or directly to secondary logs
	 * if longer than a flash page Merges consecutive log entries of the same
	 * node to limit the number of write accesses
	 * @param p_buffer
	 *            data block
	 * @param p_offset
	 *            offset within the buffer
	 * @param p_length
	 *            length of data
	 * @param p_lengthByBackupRange
	 *            length of data per node
	 * @throws IOException
	 *             if secondary log (buffer) could not be written
	 * @throws InterruptedException
	 *             if caller is interrupted
	 * @return the number of stored bytes
	 */
	private int bufferAndStoreSegmentsHashSort(final byte[] p_buffer, final int p_offset, final int p_length,
			final Set<Entry<Long, Integer>> p_lengthByBackupRange) throws InterruptedException, IOException {
		int i = 0;
		int offset = 0;
		int bufferOffset = p_offset;
		int primaryLogBufferOffset = 0;
		int primaryLogBufferSize = 0;
		int bytesRead = 0;
		int logEntrySize;
		int bytesUntilEnd;
		int length;
		short source;
		long rangeID;
		byte[] primaryLogBuffer;
		byte[] header;
		byte[] segment;
		LogEntryHeaderInterface logEntryHeader;
		HashMap<Long, BufferSegmentsNode> map;
		Iterator<Entry<Long, Integer>> iter;
		Entry<Long, Integer> entry;
		Iterator<Entry<Long, BufferSegmentsNode>> iter2;
		Entry<Long, BufferSegmentsNode> entry2;
		BufferSegmentsNode bufferNode;

		// Sort buffer by backup range
		if (p_buffer.length >= 0) {

			/*
			 * Initialize backup range buffers:
			 * For every NodeID with at least one log entry in this
			 * buffer a hashmap entry will be created. The hashmap entry
			 * contains the RangeID (key), a buffer fitting all log
			 * entries and an offset. The size of the buffer is known
			 * from the monitoring information p_lengthByBackupRange.
			 * The offset is zero if the buffer will be stored in primary
			 * log (complete header) The offset is two if the buffer will be
			 * stored directly in secondary log (header without NodeID).
			 */
			map = new HashMap<Long, BufferSegmentsNode>();
			iter = p_lengthByBackupRange.iterator();
			while (iter.hasNext()) {
				entry = iter.next();
				rangeID = entry.getKey();
				length = entry.getValue();
				if (length < LogHandler.FLASHPAGE_SIZE) {
					// There is less than 4096KB data from this node ->
					// store buffer in primary log (later)
					primaryLogBufferSize += length;
					bufferNode = new BufferSegmentsNode(length, false);
				} else {
					bufferNode = new BufferSegmentsNode(length, true);
				}
				map.put(rangeID, bufferNode);
			}

			while (bytesRead < p_length) {
				bytesUntilEnd = p_buffer.length - (bufferOffset + offset);
				if (bytesUntilEnd > 0) {
					logEntryHeader = AbstractLogEntryHeader.getPrimaryHeader(p_buffer, bufferOffset + offset);
				} else {
					logEntryHeader = AbstractLogEntryHeader.getPrimaryHeader(p_buffer, -bytesUntilEnd);
				}

				/*
				 * Because of the log's wrap around three cases must be
				 * distinguished 1. Complete entry fits in current iteration 2.
				 * Offset pointer is already in next iteration 3. Log entry must
				 * be split over two iterations
				 */
				if (bytesUntilEnd > logEntryHeader.getVEROffset()) {
					logEntrySize = logEntryHeader.getHeaderSize() + logEntryHeader.getLength(p_buffer, bufferOffset + offset);
					if (logEntryHeader instanceof MigrationPrimLogEntryHeader || logEntryHeader instanceof MigrationPrimLogTombstone) {
						rangeID = ((long) -1 << 48) + logEntryHeader.getRangeID(p_buffer, bufferOffset + offset);
					} else {
						rangeID = m_logHandler.getBackupRange(logEntryHeader.getChunkID(p_buffer, bufferOffset + offset));
					}

					bufferNode = map.get(rangeID);
					if (logEntryHeader instanceof MigrationPrimLogEntryHeader || logEntryHeader instanceof MigrationPrimLogTombstone) {
						bufferNode.putSource(logEntryHeader.getSource(p_buffer, bufferOffset + offset));
					}
					bufferNode.appendToBuffer(p_buffer, bufferOffset + offset, logEntrySize, bytesUntilEnd);

					offset += logEntrySize;
				} else if (bytesUntilEnd <= 0) {
					// Buffer overflow -> header is near the beginning
					logEntrySize = logEntryHeader.getHeaderSize() + logEntryHeader.getLength(p_buffer, -bytesUntilEnd);
					if (logEntryHeader instanceof MigrationPrimLogEntryHeader || logEntryHeader instanceof MigrationPrimLogTombstone) {
						rangeID = ((long) -1 << 48) + logEntryHeader.getRangeID(p_buffer, -bytesUntilEnd);
					} else {
						rangeID = m_logHandler.getBackupRange(logEntryHeader.getChunkID(p_buffer, -bytesUntilEnd));
					}

					bufferNode = map.get(rangeID);
					if (logEntryHeader instanceof MigrationPrimLogEntryHeader || logEntryHeader instanceof MigrationPrimLogTombstone) {
						bufferNode.putSource(logEntryHeader.getSource(p_buffer, -bytesUntilEnd));
					}
					bufferNode.appendToBuffer(p_buffer, -bytesUntilEnd, logEntrySize, bytesUntilEnd);

					bufferOffset = 0;
					offset = logEntrySize - bytesUntilEnd;
				} else {
					// Buffer overflow -> header is split
					header = new byte[logEntryHeader.getHeaderSize()];
					System.arraycopy(p_buffer, bufferOffset + offset, header, 0, bytesUntilEnd);
					System.arraycopy(p_buffer, 0, header, bytesUntilEnd, logEntryHeader.getHeaderSize() - bytesUntilEnd);

					logEntrySize = logEntryHeader.getHeaderSize() + logEntryHeader.getLength(header, 0);
					if (logEntryHeader instanceof MigrationPrimLogEntryHeader || logEntryHeader instanceof MigrationPrimLogTombstone) {
						rangeID = ((long) -1 << 48) + logEntryHeader.getRangeID(header, 0);
					} else {
						rangeID = m_logHandler.getBackupRange(logEntryHeader.getChunkID(header, 0));
					}

					bufferNode = map.get(rangeID);
					if (logEntryHeader instanceof MigrationPrimLogEntryHeader || logEntryHeader instanceof MigrationPrimLogTombstone) {
						bufferNode.putSource(logEntryHeader.getSource(header, 0));
					}
					bufferNode.appendToBuffer(p_buffer, bufferOffset + offset, logEntrySize, bytesUntilEnd);

					bufferOffset = 0;
					offset = logEntrySize - bytesUntilEnd;
				}
				bytesRead += logEntrySize;
			}

			// Write sorted buffers to log
			primaryLogBuffer = new byte[primaryLogBufferSize];

			iter2 = map.entrySet().iterator();
			while (iter2.hasNext()) {
				i = 0;
				entry2 = iter2.next();
				rangeID = entry2.getKey();
				bufferNode = entry2.getValue();
				bufferNode.trimLastSegment();
				segment = bufferNode.getData(i);
				length = bufferNode.getLength(i);
				source = bufferNode.getSource();

				while (segment != null) {
					if (length < LogHandler.FLASHPAGE_SIZE) {
						// 1. Buffer in secondary log buffer
						bufferLogEntryInSecondaryLogBuffer(segment, 0, length, rangeID, source);
						// 2. Copy log entry/range to write it in primary log subsequently
						System.arraycopy(segment, 0, primaryLogBuffer, primaryLogBufferOffset, length);
						primaryLogBufferOffset += length;
					} else {
						// Segment is larger than one flash page -> skip primary log
						writeDirectlyToSecondaryLog(segment, 0, length, rangeID, source);
					}
					segment = bufferNode.getData(++i);
					length = bufferNode.getLength(i);
				}
				iter2.remove();
				bufferNode = null;
			}

			if (primaryLogBufferSize > 0) {
				// Write all log entries, that were not written to secondary
				// log, in primary log with one write access
				if (getWritableSpace() < primaryLogBufferOffset) {
					// Not enough free space in primary log -> flush to
					// secondary logs and reset primary log
					m_logHandler.flushDataToSecondaryLogs();
					resetLog();
				}

				appendToLog(primaryLogBuffer, 0, primaryLogBufferOffset, false);
				primaryLogBuffer = null;
			}
		}

		return bytesRead;
	}

	/**
	 * Writes given data to secondary log buffers or directly to secondary logs
	 * if longer than a flash page Merges consecutive log entries of the same
	 * node to limit the number of write accesses
	 * @param p_buffer
	 *            data block
	 * @param p_offset
	 *            offset within the buffer
	 * @param p_length
	 *            length of data
	 * @param p_lengthByNode
	 *            length of data per node
	 * @throws IOException
	 *             if secondary log (buffer) could not be written
	 * @throws InterruptedException
	 *             if caller is interrupted
	 * @return the number of stored bytes
	 */
	/*
	 * private int bufferAndStoreHashSort(final byte[] p_buffer,
	 * final int p_offset, final int p_length, final int[] p_lengthByNode)
	 * throws InterruptedException, IOException {
	 * final int logHeaderSize = LogHandler.PRIMARY_HEADER_SIZE;
	 * short nodeID;
	 * int offset = 0;
	 * int bufferOffset = p_offset;
	 * int nidOffset;
	 * int primaryLogBufferOffset = 0;
	 * int primaryLogBufferSize = 0;
	 * int bytesRead = 0;
	 * int logEntrySize;
	 * int bytesUntilEnd;
	 * int length;
	 * byte[] primaryLogBuffer;
	 * byte[] header;
	 * byte[] buffer;
	 * HashMap<Short, BufferNode> map;
	 * Iterator<Entry<Short, BufferNode>> iter;
	 * Entry<Short, BufferNode> entry;
	 * BufferNode bufferNode;
	 * if (p_buffer.length >= logHeaderSize) {
	 * // Sort buffer by NodeID
	 * map = new HashMap<Short, BufferNode>();
	 * while (bytesRead < p_length) {
	 * bytesUntilEnd = p_buffer.length - (bufferOffset + offset);
	 * Because of the log's wrap around three cases must be
	 * distinguished 1. Complete entry fits in current iteration 2.
	 * Offset pointer is already in next iteration 3. Log entry must
	 * be split over two iterations
	 * if (bytesUntilEnd > LogHandler.PRIMARY_HEADER_LEN_OFFSET
	 * + LogHandler.LOG_HEADER_LEN_SIZE) {
	 * // Determine header of next log entry
	 * logEntrySize = logHeaderSize
	 * + getLengthOfLogEntry(p_buffer, bufferOffset
	 * + offset, true);
	 * nodeID = getNodeIDOfLogEntry(p_buffer, bufferOffset
	 * + offset);
	 * For every NodeID with at least one log entry in this
	 * buffer a hashmap entry will be created The hashmap entry
	 * contains the NodeID (key), a buffer fitting all log
	 * entries and an offset The size of the buffer is known
	 * from the monitoring information p_lengthByNode The offset
	 * is zero if the buffer will be stored in primary log
	 * (complete header) The offset is two if the buffer will be
	 * stored directly in secondary log (header without NodeID)
	 * bufferNode = map.get(nodeID);
	 * if (bufferNode == null) {
	 * length = p_lengthByNode[nodeID & 0xFFFF];
	 * if (length < LogHandler.FLASHPAGE_SIZE) {
	 * // There is less than 4096KB data from this node ->
	 * // store buffer in primary log (later)
	 * primaryLogBufferSize += length;
	 * nidOffset = 0;
	 * } else {
	 * nidOffset = LogHandler.LOG_HEADER_NID_SIZE;
	 * }
	 * bufferNode = new BufferNode(nidOffset, new byte[length]);
	 * map.put(nodeID, bufferNode);
	 * }
	 * bufferNode.appendToBuffer(p_buffer, bufferOffset + offset,
	 * logEntrySize, bytesUntilEnd);
	 * offset += logEntrySize;
	 * } else if (bytesUntilEnd <= 0) {
	 * // Buffer overflow -> header is near the beginning
	 * logEntrySize = logHeaderSize
	 * + getLengthOfLogEntry(p_buffer, -bytesUntilEnd,
	 * true);
	 * nodeID = getNodeIDOfLogEntry(p_buffer, -bytesUntilEnd);
	 * bufferNode = map.get(nodeID);
	 * if (bufferNode == null) {
	 * length = p_lengthByNode[nodeID & 0xFFFF];
	 * if (length < LogHandler.FLASHPAGE_SIZE) {
	 * primaryLogBufferSize += length;
	 * nidOffset = 0;
	 * } else {
	 * nidOffset = LogHandler.LOG_HEADER_NID_SIZE;
	 * }
	 * bufferNode = new BufferNode(nidOffset, new byte[length]);
	 * map.put(nodeID, bufferNode);
	 * }
	 * bufferNode.appendToBuffer(p_buffer, -bytesUntilEnd,
	 * logEntrySize, bytesUntilEnd);
	 * bufferOffset = 0;
	 * offset = logEntrySize - bytesUntilEnd;
	 * } else {
	 * // Buffer overflow -> header is split
	 * header = new byte[logHeaderSize];
	 * System.arraycopy(p_buffer, bufferOffset + offset, header,
	 * 0, bytesUntilEnd);
	 * System.arraycopy(p_buffer, 0, header, bytesUntilEnd,
	 * logHeaderSize - bytesUntilEnd);
	 * logEntrySize = logHeaderSize
	 * + getLengthOfLogEntry(header, 0, true);
	 * nodeID = getNodeIDOfLogEntry(header, 0);
	 * bufferNode = map.get(nodeID);
	 * if (bufferNode == null) {
	 * length = p_lengthByNode[nodeID & 0xFFFF];
	 * if (length < LogHandler.FLASHPAGE_SIZE) {
	 * primaryLogBufferSize += length;
	 * nidOffset = 0;
	 * } else {
	 * nidOffset = LogHandler.LOG_HEADER_NID_SIZE;
	 * }
	 * bufferNode = new BufferNode(nidOffset, new byte[length]);
	 * map.put(nodeID, bufferNode);
	 * }
	 * bufferNode.appendToBuffer(p_buffer, bufferOffset + offset,
	 * logEntrySize, bytesUntilEnd);
	 * bufferOffset = 0;
	 * offset = logEntrySize - bytesUntilEnd;
	 * }
	 * bytesRead += logEntrySize;
	 * }
	 * // Write sorted buffers to log
	 * primaryLogBuffer = new byte[primaryLogBufferSize];
	 * iter = map.entrySet().iterator();
	 * while (iter.hasNext()) {
	 * entry = iter.next();
	 * nodeID = entry.getKey();
	 * bufferNode = entry.getValue();
	 * length = bufferNode.getLength();
	 * buffer = bufferNode.getData();
	 * if (length < LogHandler.FLASHPAGE_SIZE) {
	 * // 1. Buffer in secondary log buffer
	 * bufferLogEntryInSecondaryLogBuffer(buffer, 0, length,
	 * nodeID);
	 * // 2. Copy log entry/range to write it in primary log
	 * // subsequently
	 * System.arraycopy(buffer, 0, primaryLogBuffer,
	 * primaryLogBufferOffset, length);
	 * primaryLogBufferOffset += length;
	 * } else {
	 * // Buffer contains an object/range that is bigger than one
	 * // flash page -> skip primary log
	 * writeDirectlyToSecondaryLog(buffer, 0, length, nodeID);
	 * }
	 * iter.remove();
	 * bufferNode = null;
	 * }
	 * if (primaryLogBufferSize > 0) {
	 * // Write all log entries, that were not written to secondary
	 * // log, in primary log with one write access
	 * if (getWritableSpace() < primaryLogBufferOffset) {
	 * // Not enough free space in primary log -> flush to
	 * // secondary logs and reset primary log
	 * m_logHandler.flushDataToSecondaryLogs();
	 * resetLog();
	 * }
	 * appendToLog(primaryLogBuffer, 0, primaryLogBufferOffset);
	 * primaryLogBuffer = null;
	 * }
	 * }
	 * return bytesRead;
	 * }
	 */

	/**
	 * Buffers an log entry or log entry range in corresponding secondary log
	 * buffer
	 * @param p_buffer
	 *            data block
	 * @param p_bufferOffset
	 *            position of log entry/range in data block
	 * @param p_logEntrySize
	 *            size of log entry/range
	 * @param p_chunkID
	 *            ChunkID of log entry/range
	 * @param p_source
	 *            the source NodeID
	 * @throws IOException
	 *             if secondary log buffer could not be written
	 * @throws InterruptedException
	 *             if caller is interrupted
	 */
	private void bufferLogEntryInSecondaryLogBuffer(final byte[] p_buffer, final int p_bufferOffset, final int p_logEntrySize, final long p_chunkID,
			final short p_source) throws IOException, InterruptedException {

		if (ChunkID.getCreatorID(p_chunkID) == -1) {
			m_logHandler.getSecondaryLogBuffer(p_chunkID, p_source, (byte) p_chunkID).bufferData(p_buffer, p_bufferOffset, p_logEntrySize);
		} else {
			m_logHandler.getSecondaryLogBuffer(p_chunkID, p_source, (byte) -1).bufferData(p_buffer, p_bufferOffset, p_logEntrySize);
		}
	}

	/**
	 * Writes a log entry/range directly to secondary log buffer if longer than
	 * one flash page Has to flush the corresponding secondary log buffer if not
	 * empty to maintain order
	 * @param p_buffer
	 *            data block
	 * @param p_bufferOffset
	 *            position of log entry/range in data block
	 * @param p_logEntrySize
	 *            size of log entry/range
	 * @param p_chunkID
	 *            ChunkID of log entry/range
	 * @param p_source
	 *            the source NodeID
	 * @throws IOException
	 *             if secondary log could not be written
	 * @throws InterruptedException
	 *             if caller is interrupted
	 */
	private void writeDirectlyToSecondaryLog(final byte[] p_buffer, final int p_bufferOffset, final int p_logEntrySize, final long p_chunkID,
			final short p_source) throws IOException, InterruptedException {

		if (ChunkID.getCreatorID(p_chunkID) == -1) {
			m_logHandler.getSecondaryLogBuffer(p_chunkID, p_source, (byte) p_chunkID).flushAllDataToSecLog(p_buffer, p_bufferOffset, p_logEntrySize);
		} else {
			m_logHandler.getSecondaryLogBuffer(p_chunkID, p_source, (byte) -1).flushAllDataToSecLog(p_buffer, p_bufferOffset, p_logEntrySize);
		}
	}

	// Classes
	/**
	 * BufferNode
	 * @author Kevin Beineke 11.08.2014
	 */
	public class BufferNode {

		// Attributes
		private int m_length;
		private int m_nidOffset;
		private byte[] m_data;

		// Constructors
		/**
		 * Creates an instance of BufferNode
		 * @param p_nidOffset
		 *            the header offset (with or without NodeID)
		 * @param p_data
		 *            the buffer
		 */
		public BufferNode(final int p_nidOffset, final byte[] p_data) {
			m_length = 0;
			m_nidOffset = p_nidOffset;
			m_data = p_data;
		}

		// Getter
		/**
		 * Returns the number of written bytes
		 * @return the number of written bytes
		 */
		public final int getLength() {
			return m_length;
		}

		/**
		 * Returns the buffer
		 * @return the buffer
		 */
		public final byte[] getData() {
			return m_data;
		}

		// Methods
		/**
		 * Appends data to node buffer
		 * @param p_buffer
		 *            the buffer
		 * @param p_offset
		 *            the offset within the buffer
		 * @param p_logEntrySize
		 *            the log entry size
		 * @param p_bytesUntilEnd
		 *            the number of bytes until end
		 */
		public final void appendToBuffer(final byte[] p_buffer, final int p_offset, final int p_logEntrySize, final int p_bytesUntilEnd) {
			if (p_bytesUntilEnd >= p_logEntrySize || p_bytesUntilEnd <= 0) {
				System.arraycopy(p_buffer, p_offset + m_nidOffset, m_data, m_length, p_logEntrySize - m_nidOffset);
			} else {
				if (p_bytesUntilEnd > m_nidOffset) {
					System.arraycopy(p_buffer, p_offset + m_nidOffset, m_data, m_length, p_bytesUntilEnd - m_nidOffset);
					System.arraycopy(p_buffer, 0, m_data, m_length + p_bytesUntilEnd - m_nidOffset, p_logEntrySize - p_bytesUntilEnd);
				} else {
					System.arraycopy(p_buffer, 0, m_data, m_length + m_nidOffset - p_bytesUntilEnd, p_logEntrySize - (m_nidOffset - p_bytesUntilEnd));
				}
			}
			m_length += p_logEntrySize - m_nidOffset;
		}
	}

	/**
	 * BufferNode
	 * @author Kevin Beineke 11.08.2014
	 */
	public class BufferSegmentsNode {

		// Attributes
		private short m_source;
		private int m_numberOfSegments;
		private int m_currentSegment;
		private int m_startIndex;
		private boolean m_convert;
		private int[] m_writtenBytesPerSegment;
		private boolean[] m_filledSegments;
		private byte[][] m_segments;

		// Constructors
		/**
		 * Creates an instance of BufferNode
		 * @param p_length
		 *            the buffer length
		 * @param p_convert
		 *            wether the log entry headers have to be converted or not
		 */
		public BufferSegmentsNode(final int p_length, final boolean p_convert) {
			m_source = -1;

			m_numberOfSegments = (int) Math.ceil((double) p_length / LogHandler.SECLOG_SEGMENT_SIZE);

			m_currentSegment = 0;
			m_startIndex = 0;
			m_convert = p_convert;

			m_writtenBytesPerSegment = new int[m_numberOfSegments];
			m_filledSegments = new boolean[m_numberOfSegments];
			m_segments = new byte[m_numberOfSegments][];

			for (int i = 0; i < m_segments.length; i++) {
				m_segments[i] = new byte[LogHandler.SECLOG_SEGMENT_SIZE];
			}
		}

		// Getter
		/**
		 * Returns the number of written bytes per segment
		 * @param p_index
		 *            the index
		 * @return the number of written bytes per segment
		 */
		public final int getLength(final int p_index) {
			int ret = 0;

			if (p_index < m_numberOfSegments) {
				ret = m_writtenBytesPerSegment[p_index];
			}

			return ret;
		}

		/**
		 * Returns the buffer
		 * @param p_index
		 *            the index
		 * @return the buffer
		 */
		public final byte[] getData(final int p_index) {
			byte[] ret = null;

			if (p_index < m_numberOfSegments) {
				ret = m_segments[p_index];
			}

			return ret;
		}

		/**
		 * Returns the source
		 * @return the NodeID
		 */
		public final short getSource() {
			return m_source;
		}

		// Setter
		/**
		 * Puts the source
		 * @param p_source
		 *            the NodeID
		 */
		public final void putSource(final short p_source) {
			m_source = p_source;
		}

		// Methods
		/**
		 * Appends data to node buffer
		 * @param p_buffer
		 *            the buffer
		 * @param p_offset
		 *            the offset within the buffer
		 * @param p_logEntrySize
		 *            the log entry size
		 * @param p_bytesUntilEnd
		 *            the number of bytes until end
		 */
		public final void appendToBuffer(final byte[] p_buffer, final int p_offset, final int p_logEntrySize, final int p_bytesUntilEnd) {
			int index = -1;
			LogEntryHeaderInterface logEntryHeader;

			logEntryHeader = AbstractLogEntryHeader.getPrimaryHeader(p_buffer, p_offset);

			for (int i = m_startIndex; i <= m_currentSegment; i++) {
				if (LogHandler.SECLOG_SEGMENT_SIZE - m_writtenBytesPerSegment[i] >= p_logEntrySize) {
					index = i;
					break;
				} else if (LogHandler.SECLOG_SEGMENT_SIZE - m_writtenBytesPerSegment[i] <= DEFAULT_SEC_LOG_ENTRY_HEADER.getHeaderSize(true)) {
					m_filledSegments[i] = true;
					for (int j = m_startIndex; j <= m_currentSegment; j++) {
						if (m_filledSegments[j]) {
							m_startIndex = j + 1;
						} else {
							break;
						}
					}
				}
			}
			if (index == -1) {
				index = ++m_currentSegment;
				if (m_currentSegment == m_numberOfSegments) {
					// Add a segment because of fragmentation
					m_segments = Arrays.copyOf(m_segments, ++m_numberOfSegments);
					m_writtenBytesPerSegment = Arrays.copyOf(m_writtenBytesPerSegment, m_numberOfSegments);
					m_filledSegments = Arrays.copyOf(m_filledSegments, m_numberOfSegments);
					m_segments[m_currentSegment] = new byte[LogHandler.SECLOG_SEGMENT_SIZE];
				}
			}

			if (m_convert) {
				// More than one page for this node: Convert primary log entry header to secondary log header and append
				// entry to node buffer
				if (logEntryHeader instanceof MigrationPrimLogEntryHeader || logEntryHeader instanceof MigrationPrimLogTombstone) {
					// Secondary log entry header for migration contains the creator's NodeID, the normal header does
					// not
					AbstractLogEntryHeader.convertAndPut(p_buffer, p_offset, m_segments[index], m_writtenBytesPerSegment[index], p_logEntrySize,
							p_bytesUntilEnd, logEntryHeader.getConversionOffset());
					m_writtenBytesPerSegment[index] += p_logEntrySize - logEntryHeader.getConversionOffset();
				} else {
					AbstractLogEntryHeader.convertAndPut(p_buffer, p_offset, m_segments[index], m_writtenBytesPerSegment[index], p_logEntrySize,
							p_bytesUntilEnd, logEntryHeader.getConversionOffset());
					m_writtenBytesPerSegment[index] += p_logEntrySize - logEntryHeader.getConversionOffset();
				}
			} else {
				// Less than one page for this node: Just append entry to node buffer without converting the log entry
				// header
				if (p_bytesUntilEnd >= p_logEntrySize || p_bytesUntilEnd <= 0) {
					System.arraycopy(p_buffer, p_offset, m_segments[index], m_writtenBytesPerSegment[index], p_logEntrySize);
				} else {
					System.arraycopy(p_buffer, p_offset, m_segments[index], m_writtenBytesPerSegment[index], p_bytesUntilEnd);
					System.arraycopy(p_buffer, 0, m_segments[index], m_writtenBytesPerSegment[index] + p_bytesUntilEnd, p_logEntrySize - p_bytesUntilEnd);
				}
				m_writtenBytesPerSegment[index] += p_logEntrySize;
			}

		}

		/**
		 * Trims the last segment
		 */
		public final void trimLastSegment() {
			int length;

			length = m_writtenBytesPerSegment[m_numberOfSegments - 1];
			if (length == 0) {
				m_segments = Arrays.copyOf(m_segments, --m_numberOfSegments);
				m_writtenBytesPerSegment = Arrays.copyOf(m_writtenBytesPerSegment, m_numberOfSegments);
				trimLastSegment();
			} else {
				m_segments[m_numberOfSegments - 1] = Arrays.copyOf(m_segments[m_numberOfSegments - 1], length);
			}
		}
	}
}
