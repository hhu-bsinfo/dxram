
package de.uniduesseldorf.dxram.core.log.storage;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import de.uniduesseldorf.dxram.core.CoreComponentFactory;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.log.LogHandler;
import de.uniduesseldorf.dxram.core.log.LogInterface;

/**
 * This class implements the primary log. Furthermore this class manages all
 * secondary logs
 * @author Kevin Beineke 13.06.2014
 */
public final class PrimaryLog extends AbstractLog implements LogStorageInterface {

	// Constants

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
	public PrimaryLog(final long p_primaryLogSize) throws IOException,
			InterruptedException {
		super(new File(LogHandler.BACKUP_DIRECTORY + "N"
				+ NodeID.getLocalNodeID() + "_"
				+ LogHandler.PRIMARYLOG_FILENAME), p_primaryLogSize,
				LogHandler.PRIMLOG_HEADER_SIZE);

		try {
			m_logHandler = CoreComponentFactory.getLogInterface();
		} catch (final DXRAMException e) {
			System.out.println("Could not get log interface");
		}

		if (p_primaryLogSize < LogHandler.PRIMARY_LOG_MIN_SIZE) {
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

	@Override
	public int appendData(final byte[] p_data, final int p_offset,
			final int p_length, final Object p_lengthByNode)
			throws IOException, InterruptedException {
		int ret = 0;

		if (p_length <= 0 || p_length > m_totalUsableSpace) {
			throw new IllegalArgumentException("invalid data size");
		} else {
			ret = bufferAndStoreSegmentsHashSort(p_data, p_offset, p_length,
					(int[]) p_lengthByNode);
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
	 * @param p_lengthByNode
	 *            length of data per node
	 * @throws IOException
	 *             if secondary log (buffer) could not be written
	 * @throws InterruptedException
	 *             if caller is interrupted
	 * @return the number of stored bytes
	 */
	private int bufferAndStoreSegmentsHashSort(final byte[] p_buffer,
			final int p_offset, final int p_length, final int[] p_lengthByNode)
			throws InterruptedException, IOException {
		final int logHeaderSize = LogHandler.PRIMARY_HEADER_SIZE;
		short nodeID;
		int i = 0;
		int offset = 0;
		int bufferOffset = p_offset;
		int nidOffset;
		int primaryLogBufferOffset = 0;
		int primaryLogBufferSize = 0;
		int bytesRead = 0;
		int logEntrySize;
		int bytesUntilEnd;
		int length;
		byte[] primaryLogBuffer;
		byte[] header;
		byte[] segment;
		HashMap<Short, BufferSegmentsNode> map;
		Iterator<Entry<Short, BufferSegmentsNode>> iter;
		Entry<Short, BufferSegmentsNode> entry;
		BufferSegmentsNode bufferNode;

		if (p_buffer.length >= logHeaderSize) {
			// Sort buffer by NodeID
			map = new HashMap<Short, BufferSegmentsNode>();
			while (bytesRead < p_length) {
				bytesUntilEnd = p_buffer.length - (bufferOffset + offset);

				/*
				 * Because of the log's wrap around three cases must be
				 * distinguished 1. Complete entry fits in current iteration 2.
				 * Offset pointer is already in next iteration 3. Log entry must
				 * be split over two iterations
				 */
				if (bytesUntilEnd > LogHandler.PRIMARY_HEADER_LEN_OFFSET
						+ LogHandler.LOG_HEADER_LEN_SIZE) {
					// Determine header of next log entry
					logEntrySize = logHeaderSize
							+ getLengthOfLogEntry(p_buffer, bufferOffset
									+ offset, true);
					nodeID = getNodeIDOfLogEntry(p_buffer, bufferOffset
							+ offset);

					/*
					 * For every NodeID with at least one log entry in this
					 * buffer a hashmap entry will be created The hashmap entry
					 * contains the NodeID (key), a buffer fitting all log
					 * entries and an offset The size of the buffer is known
					 * from the monitoring information p_lengthByNode The offset
					 * is zero if the buffer will be stored in primary log
					 * (complete header) The offset is two if the buffer will be
					 * stored directly in secondary log (header without NodeID)
					 */
					bufferNode = map.get(nodeID);
					if (bufferNode == null) {
						length = p_lengthByNode[nodeID & 0xFFFF];
						if (length < LogHandler.FLASHPAGE_SIZE) {
							// There is less than 4096KB data from this node ->
							// store buffer in primary log (later)
							primaryLogBufferSize += length;
							nidOffset = 0;
						} else {
							nidOffset = LogHandler.LOG_HEADER_NID_SIZE;
						}
						bufferNode = new BufferSegmentsNode(nidOffset, length);
						map.put(nodeID, bufferNode);
					}
					bufferNode.appendToBuffer(p_buffer, bufferOffset + offset,
							logEntrySize, bytesUntilEnd);

					offset += logEntrySize;
				} else if (bytesUntilEnd <= 0) {
					// Buffer overflow -> header is near the beginning
					logEntrySize = logHeaderSize
							+ getLengthOfLogEntry(p_buffer, -bytesUntilEnd,
									true);
					nodeID = getNodeIDOfLogEntry(p_buffer, -bytesUntilEnd);

					bufferNode = map.get(nodeID);
					if (bufferNode == null) {
						length = p_lengthByNode[nodeID & 0xFFFF];
						if (length < LogHandler.FLASHPAGE_SIZE) {
							primaryLogBufferSize += length;
							nidOffset = 0;
						} else {
							nidOffset = LogHandler.LOG_HEADER_NID_SIZE;
						}
						bufferNode = new BufferSegmentsNode(nidOffset, length);
						map.put(nodeID, bufferNode);
					}
					bufferNode.appendToBuffer(p_buffer, -bytesUntilEnd,
							logEntrySize, bytesUntilEnd);

					bufferOffset = 0;
					offset = logEntrySize - bytesUntilEnd;
				} else {
					// Buffer overflow -> header is split
					header = new byte[logHeaderSize];

					System.arraycopy(p_buffer, bufferOffset + offset, header,
							0, bytesUntilEnd);
					System.arraycopy(p_buffer, 0, header, bytesUntilEnd,
							logHeaderSize - bytesUntilEnd);
					logEntrySize = logHeaderSize
							+ getLengthOfLogEntry(header, 0, true);
					nodeID = getNodeIDOfLogEntry(header, 0);

					bufferNode = map.get(nodeID);
					if (bufferNode == null) {
						length = p_lengthByNode[nodeID & 0xFFFF];
						if (length < LogHandler.FLASHPAGE_SIZE) {
							primaryLogBufferSize += length;
							nidOffset = 0;
						} else {
							nidOffset = LogHandler.LOG_HEADER_NID_SIZE;
						}
						bufferNode = new BufferSegmentsNode(nidOffset, length);
						map.put(nodeID, bufferNode);
					}
					bufferNode.appendToBuffer(p_buffer, bufferOffset + offset,
							logEntrySize, bytesUntilEnd);

					bufferOffset = 0;
					offset = logEntrySize - bytesUntilEnd;
				}
				bytesRead += logEntrySize;
			}

			// Write sorted buffers to log
			primaryLogBuffer = new byte[primaryLogBufferSize];

			iter = map.entrySet().iterator();
			while (iter.hasNext()) {
				i = 0;
				entry = iter.next();
				nodeID = entry.getKey();
				bufferNode = entry.getValue();
				bufferNode.trimLastSegment();
				segment = bufferNode.getData(i);
				length = bufferNode.getLength(i);

				while (segment != null) {
					if (length < LogHandler.FLASHPAGE_SIZE) {
						// 1. Buffer in secondary log buffer
						bufferLogEntryInSecondaryLogBuffer(segment, 0, length,
								nodeID);
						// 2. Copy log entry/range to write it in primary log
						// subsequently
						System.arraycopy(segment, 0, primaryLogBuffer,
								primaryLogBufferOffset, length);
						primaryLogBufferOffset += length;
						break;
					} else {
						// Segment is larger than one flash page -> skip primary
						// log
						writeDirectlyToSecondaryLog(segment, 0, length, nodeID);
					}
					segment = bufferNode.getData(++i);
					length = bufferNode.getLength(i);
				}
				iter.remove();
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

				appendToLog(primaryLogBuffer, 0, primaryLogBufferOffset);
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
	 *
	 * if (p_buffer.length >= logHeaderSize) {
	 * // Sort buffer by NodeID
	 * map = new HashMap<Short, BufferNode>();
	 * while (bytesRead < p_length) {
	 * bytesUntilEnd = p_buffer.length - (bufferOffset + offset);
	 *
	 *
	 * Because of the log's wrap around three cases must be
	 * distinguished 1. Complete entry fits in current iteration 2.
	 * Offset pointer is already in next iteration 3. Log entry must
	 * be split over two iterations
	 *
	 * if (bytesUntilEnd > LogHandler.PRIMARY_HEADER_LEN_OFFSET
	 * + LogHandler.LOG_HEADER_LEN_SIZE) {
	 * // Determine header of next log entry
	 * logEntrySize = logHeaderSize
	 * + getLengthOfLogEntry(p_buffer, bufferOffset
	 * + offset, true);
	 * nodeID = getNodeIDOfLogEntry(p_buffer, bufferOffset
	 * + offset);
	 *
	 *
	 * For every NodeID with at least one log entry in this
	 * buffer a hashmap entry will be created The hashmap entry
	 * contains the NodeID (key), a buffer fitting all log
	 * entries and an offset The size of the buffer is known
	 * from the monitoring information p_lengthByNode The offset
	 * is zero if the buffer will be stored in primary log
	 * (complete header) The offset is two if the buffer will be
	 * stored directly in secondary log (header without NodeID)
	 *
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
	 *
	 * offset += logEntrySize;
	 * } else if (bytesUntilEnd <= 0) {
	 * // Buffer overflow -> header is near the beginning
	 * logEntrySize = logHeaderSize
	 * + getLengthOfLogEntry(p_buffer, -bytesUntilEnd,
	 * true);
	 * nodeID = getNodeIDOfLogEntry(p_buffer, -bytesUntilEnd);
	 *
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
	 *
	 * bufferOffset = 0;
	 * offset = logEntrySize - bytesUntilEnd;
	 * } else {
	 * // Buffer overflow -> header is split
	 * header = new byte[logHeaderSize];
	 *
	 * System.arraycopy(p_buffer, bufferOffset + offset, header,
	 * 0, bytesUntilEnd);
	 * System.arraycopy(p_buffer, 0, header, bytesUntilEnd,
	 * logHeaderSize - bytesUntilEnd);
	 * logEntrySize = logHeaderSize
	 * + getLengthOfLogEntry(header, 0, true);
	 * nodeID = getNodeIDOfLogEntry(header, 0);
	 *
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
	 *
	 * bufferOffset = 0;
	 * offset = logEntrySize - bytesUntilEnd;
	 * }
	 * bytesRead += logEntrySize;
	 * }
	 *
	 * // Write sorted buffers to log
	 * primaryLogBuffer = new byte[primaryLogBufferSize];
	 *
	 * iter = map.entrySet().iterator();
	 * while (iter.hasNext()) {
	 * entry = iter.next();
	 * nodeID = entry.getKey();
	 * bufferNode = entry.getValue();
	 * length = bufferNode.getLength();
	 * buffer = bufferNode.getData();
	 *
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
	 *
	 * if (primaryLogBufferSize > 0) {
	 * // Write all log entries, that were not written to secondary
	 * // log, in primary log with one write access
	 * if (getWritableSpace() < primaryLogBufferOffset) {
	 * // Not enough free space in primary log -> flush to
	 * // secondary logs and reset primary log
	 * m_logHandler.flushDataToSecondaryLogs();
	 * resetLog();
	 * }
	 *
	 * appendToLog(primaryLogBuffer, 0, primaryLogBufferOffset);
	 * primaryLogBuffer = null;
	 * }
	 * }
	 *
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
	 * @param p_nodeID
	 *            NodeID of log entry/range
	 * @throws IOException
	 *             if secondary log buffer could not be written
	 * @throws InterruptedException
	 *             if caller is interrupted
	 */
	private void bufferLogEntryInSecondaryLogBuffer(final byte[] p_buffer,
			final int p_bufferOffset, final int p_logEntrySize,
			final short p_nodeID) throws IOException, InterruptedException {

		m_logHandler.getSecondaryLogBuffer(p_nodeID, true).bufferData(p_buffer,
				p_bufferOffset, p_logEntrySize);
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
	 * @param p_nodeID
	 *            NodeID of log entry/range
	 * @throws IOException
	 *             if secondary log could not be written
	 * @throws InterruptedException
	 *             if caller is interrupted
	 */
	private void writeDirectlyToSecondaryLog(final byte[] p_buffer,
			final int p_bufferOffset, final int p_logEntrySize,
			final short p_nodeID) throws IOException, InterruptedException {

		m_logHandler.getSecondaryLogBuffer(p_nodeID, true)
				.flushAllDataToSecLog(p_buffer, p_bufferOffset, p_logEntrySize);
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
		public final void appendToBuffer(final byte[] p_buffer,
				final int p_offset, final int p_logEntrySize,
				final int p_bytesUntilEnd) {
			if (p_bytesUntilEnd >= p_logEntrySize || p_bytesUntilEnd <= 0) {
				System.arraycopy(p_buffer, p_offset + m_nidOffset, m_data,
						m_length, p_logEntrySize - m_nidOffset);
			} else {
				if (p_bytesUntilEnd > m_nidOffset) {
					System.arraycopy(p_buffer, p_offset + m_nidOffset, m_data,
							m_length, p_bytesUntilEnd - m_nidOffset);
					System.arraycopy(p_buffer, 0, m_data, m_length
							+ p_bytesUntilEnd - m_nidOffset, p_logEntrySize
							- p_bytesUntilEnd);
				} else {
					System.arraycopy(p_buffer, 0, m_data, m_length
							+ m_nidOffset - p_bytesUntilEnd, p_logEntrySize
							- (m_nidOffset - p_bytesUntilEnd));
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
		private int m_nidOffset;
		private int m_numberOfSegments;
		private int m_currentSegment;
		private int m_startIndex;
		private int[] m_writtenBytesPerSegment;
		private boolean[] m_filledSegments;
		private byte[][] m_segments;

		// Constructors
		/**
		 * Creates an instance of BufferNode
		 * @param p_nidOffset
		 *            the header offset (with or without NodeID)
		 * @param p_length
		 *            the buffer length
		 */
		public BufferSegmentsNode(final int p_nidOffset, final int p_length) {
			m_nidOffset = p_nidOffset;
			m_numberOfSegments = (int) Math.ceil((double) p_length
					/ LogHandler.SEGMENT_SIZE);

			m_currentSegment = 0;
			m_startIndex = 0;

			m_writtenBytesPerSegment = new int[m_numberOfSegments];
			m_filledSegments = new boolean[m_numberOfSegments];
			m_segments = new byte[m_numberOfSegments][];

			for (int i = 0; i < m_segments.length; i++) {
				m_segments[i] = new byte[LogHandler.SEGMENT_SIZE];
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
		public final void appendToBuffer(final byte[] p_buffer,
				final int p_offset, final int p_logEntrySize,
				final int p_bytesUntilEnd) {
			int index = -1;
			int segmentOffset;
			byte[] segment;

			for (int i = m_startIndex; i <= m_currentSegment; i++) {
				if (LogHandler.SEGMENT_SIZE - m_writtenBytesPerSegment[i] >= p_logEntrySize) {
					index = i;
					break;
				} else if (LogHandler.SEGMENT_SIZE
						- m_writtenBytesPerSegment[i] <= LogHandler.SECONDARY_HEADER_SIZE) {
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
					m_segments = Arrays
							.copyOf(m_segments, ++m_numberOfSegments);
					m_writtenBytesPerSegment = Arrays.copyOf(
							m_writtenBytesPerSegment, m_numberOfSegments);
					m_filledSegments = Arrays.copyOf(m_filledSegments,
							m_numberOfSegments);
					m_segments[m_currentSegment] = new byte[LogHandler.SEGMENT_SIZE];
				}
			}

			segment = m_segments[index];
			segmentOffset = m_writtenBytesPerSegment[index];
			if (p_bytesUntilEnd >= p_logEntrySize || p_bytesUntilEnd <= 0) {
				System.arraycopy(p_buffer, p_offset + m_nidOffset, segment,
						segmentOffset, p_logEntrySize - m_nidOffset);
			} else {
				if (p_bytesUntilEnd > m_nidOffset) {
					System.arraycopy(p_buffer, p_offset + m_nidOffset, segment,
							segmentOffset, p_bytesUntilEnd - m_nidOffset);
					System.arraycopy(p_buffer, 0, segment, segmentOffset
							+ p_bytesUntilEnd - m_nidOffset, p_logEntrySize
							- p_bytesUntilEnd);
				} else {
					System.arraycopy(p_buffer, 0, segment, segmentOffset
							+ m_nidOffset - p_bytesUntilEnd, p_logEntrySize
							- (m_nidOffset - p_bytesUntilEnd));
				}
			}
			m_writtenBytesPerSegment[index] += p_logEntrySize - m_nidOffset;
		}

		/**
		 * Trims the last segment
		 */
		public final void trimLastSegment() {
			int length;

			length = m_writtenBytesPerSegment[m_numberOfSegments - 1];
			if (length == 0) {
				m_segments = Arrays.copyOf(m_segments, --m_numberOfSegments);
				m_writtenBytesPerSegment = Arrays.copyOf(
						m_writtenBytesPerSegment, m_numberOfSegments);
				trimLastSegment();
			} else {
				m_segments[m_numberOfSegments - 1] = Arrays.copyOf(
						m_segments[m_numberOfSegments - 1], length);
			}
		}
	}
}
