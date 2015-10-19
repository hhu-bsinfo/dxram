
package de.uniduesseldorf.dxram.core.log.storage;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.log.LogHandler;
import de.uniduesseldorf.dxram.core.log.header.AbstractLogEntryHeader;

/**
 * This class implements the primary log. Furthermore this class manages all
 * secondary logs
 * @author Kevin Beineke 13.06.2014
 */
public final class PrimaryLog extends AbstractLog {

	// Constants
	private static final String BACKUP_DIRECTORY = Core.getConfiguration().getStringValue(ConfigurationConstants.LOG_DIRECTORY);
	private static final String PRIMLOG_SUFFIX_FILENAME = "prim.log";
	private static final byte[] PRIMLOG_HEADER = "DXRAMPrimLogv1".getBytes(Charset.forName("UTF-8"));
	private static final long PRIMLOG_SIZE = Core.getConfiguration().getLongValue(ConfigurationConstants.PRIMARY_LOG_SIZE);
	private static final int PRIMLOG_MIN_SIZE = 65535 * FLASHPAGE_SIZE;
	private static final int SECLOG_SEGMENT_SIZE = Core.getConfiguration().getIntValue(ConfigurationConstants.LOG_SEGMENT_SIZE);

	// Attributes
	private LogHandler m_logHandler;

	private long m_totalUsableSpace;

	// Constructors
	/**
	 * Creates an instance of PrimaryLog with user specific configuration
	 * @param p_logHandler
	 *            the log handler
	 * @throws IOException
	 *             if primary log could not be created
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 */
	public PrimaryLog(final LogHandler p_logHandler) throws IOException, InterruptedException {
		super(new File(BACKUP_DIRECTORY + "N" + NodeID.getLocalNodeID() + "_" + PRIMLOG_SUFFIX_FILENAME), PRIMLOG_SIZE,
				PRIMLOG_HEADER.length);

		m_logHandler = p_logHandler;

		if (PRIMLOG_SIZE < PRIMLOG_MIN_SIZE) {
			throw new IllegalArgumentException("Error: Primary log too small");
		}

		m_totalUsableSpace = super.getTotalUsableSpace();

		createLogAndWriteHeader(PRIMLOG_HEADER);
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
			ret = bufferAndStore(p_data, p_offset, p_length, (Set<Entry<Long, Integer>>) p_lengthByBackupRange);
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
	private int bufferAndStore(final byte[] p_buffer, final int p_offset, final int p_length,
			final Set<Entry<Long, Integer>> p_lengthByBackupRange) throws InterruptedException, IOException {
		int i = 0;
		int offset = 0;
		int bufferOffset = p_offset;
		int completeOffset;
		int primaryLogBufferOffset = 0;
		int primaryLogBufferSize = 0;
		int bytesRead = 0;
		int logEntrySize;
		int bytesUntilEnd;
		int length;
		short headerSize;
		short source;
		long rangeID;
		byte[] primaryLogBuffer;
		byte[] header;
		byte[] segment;
		AbstractLogEntryHeader logEntryHeader;
		HashMap<Long, BufferNode> map;
		Iterator<Entry<Long, Integer>> iter;
		Entry<Long, Integer> entry;
		Iterator<Entry<Long, BufferNode>> iter2;
		Entry<Long, BufferNode> entry2;
		BufferNode bufferNode;

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
			map = new HashMap<Long, BufferNode>();
			iter = p_lengthByBackupRange.iterator();
			while (iter.hasNext()) {
				entry = iter.next();
				rangeID = entry.getKey();
				length = entry.getValue();
				if (length < FLASHPAGE_SIZE) {
					// There is less than 4096KB data from this node ->
					// store buffer in primary log (later)
					primaryLogBufferSize += length;
					bufferNode = new BufferNode(length, false);
				} else {
					bufferNode = new BufferNode(length, true);
				}
				map.put(rangeID, bufferNode);
			}

			while (bytesRead < p_length) {
				bytesUntilEnd = p_buffer.length - (bufferOffset + offset);
				if (bytesUntilEnd > 0) {
					completeOffset = bufferOffset + offset;
				} else {
					completeOffset = -bytesUntilEnd;
				}
				logEntryHeader = AbstractLogEntryHeader.getPrimaryHeader(p_buffer, completeOffset);

				/*
				 * Because of the log's wrap around three cases must be distinguished 1. Complete entry fits in current
				 * iteration 2.
				 * Offset pointer is already in next iteration 3. Log entry must be split over two iterations
				 */
				if (logEntryHeader.readable(p_buffer, completeOffset, bytesUntilEnd)) {
					logEntrySize = logEntryHeader.getHeaderSize(p_buffer, completeOffset) + logEntryHeader.getLength(p_buffer, completeOffset);
					if (logEntryHeader.wasMigrated()) {
						rangeID = ((long) -1 << 48) + logEntryHeader.getRangeID(p_buffer, completeOffset);
					} else {
						logEntryHeader.getHeaderSize(p_buffer, completeOffset);

						rangeID = m_logHandler.getBackupRange(logEntryHeader.getChunkID(p_buffer, completeOffset));
					}

					bufferNode = map.get(rangeID);
					if (logEntryHeader.wasMigrated()) {
						bufferNode.setSource(logEntryHeader.getSource(p_buffer, completeOffset));
					}
					bufferNode.appendToBuffer(p_buffer, completeOffset, logEntrySize, bytesUntilEnd);

					offset += logEntrySize;
				} else if (bytesUntilEnd <= 0) {
					// Buffer overflow -> header is near the beginning
					logEntrySize = logEntryHeader.getHeaderSize(p_buffer, completeOffset) + logEntryHeader.getLength(p_buffer, completeOffset);
					if (logEntryHeader.wasMigrated()) {
						rangeID = ((long) -1 << 48) + logEntryHeader.getRangeID(p_buffer, completeOffset);
					} else {
						rangeID = m_logHandler.getBackupRange(logEntryHeader.getChunkID(p_buffer, completeOffset));
					}

					bufferNode = map.get(rangeID);
					if (logEntryHeader.wasMigrated()) {
						bufferNode.setSource(logEntryHeader.getSource(p_buffer, completeOffset));
					}
					bufferNode.appendToBuffer(p_buffer, completeOffset, logEntrySize, bytesUntilEnd);

					bufferOffset = 0;
					offset = logEntrySize - bytesUntilEnd;
				} else {
					// Buffer overflow -> header is split
					headerSize = logEntryHeader.getHeaderSize(p_buffer, completeOffset);
					header = new byte[headerSize];
					System.arraycopy(p_buffer, bufferOffset + offset, header, 0, bytesUntilEnd);
					System.arraycopy(p_buffer, 0, header, bytesUntilEnd, headerSize - bytesUntilEnd);

					logEntrySize = headerSize + logEntryHeader.getLength(header, 0);
					if (logEntryHeader.wasMigrated()) {
						rangeID = ((long) -1 << 48) + logEntryHeader.getRangeID(header, 0);
					} else {
						rangeID = m_logHandler.getBackupRange(logEntryHeader.getChunkID(header, 0));
					}

					bufferNode = map.get(rangeID);
					if (logEntryHeader.wasMigrated()) {
						bufferNode.setSource(logEntryHeader.getSource(header, 0));
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
					if (length < FLASHPAGE_SIZE) {
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
	private final class BufferNode {

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
		private BufferNode(final int p_length, final boolean p_convert) {
			m_source = -1;

			m_numberOfSegments = (int) Math.ceil((double) p_length / SECLOG_SEGMENT_SIZE);

			m_currentSegment = 0;
			m_startIndex = 0;
			m_convert = p_convert;

			m_writtenBytesPerSegment = new int[m_numberOfSegments];
			m_filledSegments = new boolean[m_numberOfSegments];
			m_segments = new byte[m_numberOfSegments][];

			for (int i = 0; i < m_segments.length; i++) {
				m_segments[i] = new byte[SECLOG_SEGMENT_SIZE];
			}
		}

		// Getter
		/**
		 * Returns the number of written bytes per segment
		 * @param p_index
		 *            the index
		 * @return the number of written bytes per segment
		 */
		private int getLength(final int p_index) {
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
		private byte[] getData(final int p_index) {
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
		private short getSource() {
			return m_source;
		}

		// Setter
		/**
		 * Puts the source
		 * @param p_source
		 *            the NodeID
		 */
		private void setSource(final short p_source) {
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
		private void appendToBuffer(final byte[] p_buffer, final int p_offset, final int p_logEntrySize, final int p_bytesUntilEnd) {
			int index = -1;
			AbstractLogEntryHeader logEntryHeader;

			logEntryHeader = AbstractLogEntryHeader.getPrimaryHeader(p_buffer, p_offset);

			for (int i = m_startIndex; i <= m_currentSegment; i++) {
				if (SECLOG_SEGMENT_SIZE - m_writtenBytesPerSegment[i] >= p_logEntrySize) {
					index = i;
					break;
				} else if (p_logEntrySize - logEntryHeader.getConversionOffset() + 1 > SECLOG_SEGMENT_SIZE - m_writtenBytesPerSegment[i]) {
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
					m_segments[m_currentSegment] = new byte[SECLOG_SEGMENT_SIZE];
				}
			}

			if (m_convert) {
				// More than one page for this node: Convert primary log entry header to secondary log header and append
				// entry to node buffer
				m_writtenBytesPerSegment[index] += AbstractLogEntryHeader.convertAndPut(p_buffer, p_offset, m_segments[index],
						m_writtenBytesPerSegment[index], p_logEntrySize, p_bytesUntilEnd, logEntryHeader);
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
		private void trimLastSegment() {
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
