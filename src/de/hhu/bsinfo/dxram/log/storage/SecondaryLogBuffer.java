
package de.hhu.bsinfo.dxram.log.storage;

import java.io.IOException;
import java.util.Arrays;

import de.hhu.bsinfo.dxram.log.header.AbstractLogEntryHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class implements the secondary log buffer
 *
 * @author Kevin Beineke
 *         20.06.2014
 */
public final class SecondaryLogBuffer {

	private static final Logger LOGGER = LogManager.getFormatterLogger(SecondaryLogBuffer.class.getSimpleName());

	// Attributes
	private byte[] m_buffer;
	private int m_bytesInBuffer;

	private SecondaryLog m_secondaryLog;
	private int m_logSegmentSize;

	// Constructors

	/**
	 * Creates an instance of SecondaryLogBuffer
	 *
	 * @param p_secondaryLog   Instance of the corresponding secondary log. Used to write directly to secondary
	 * @param p_bufferSize     the secondary log buffer size
	 * @param p_logSegmentSize the segment size
	 */
	public SecondaryLogBuffer(final SecondaryLog p_secondaryLog, final int p_bufferSize,
			final int p_logSegmentSize) {

		m_secondaryLog = p_secondaryLog;
		m_logSegmentSize = p_logSegmentSize;

		m_bytesInBuffer = 0;
		m_buffer = new byte[p_bufferSize];
		// #if LOGGER == TRACE
		LOGGER.trace("Initialized secondary log buffer (%d)", p_bufferSize);
		// #endif /* LOGGER == TRACE */
	}

	// Getter

	/**
	 * Returns the number of bytes
	 *
	 * @return the number of bytes
	 */
	public int getOccupiedSpace() {
		return m_bytesInBuffer;
	}

	/**
	 * Returns whether the secondary log buffer is empty or not
	 *
	 * @return whether buffer is empty or not
	 */
	public boolean isBufferEmpty() {
		return m_bytesInBuffer == 0;
	}

	// Methods

	/**
	 * Closes the buffer
	 */
	public void close() {
		try {
			flushSecLogBuffer();
		} catch (final IOException | InterruptedException e) {

			// #if LOGGER >= ERROR
			LOGGER.error("Could not flush secondary log buffer", e);
			// #endif /* LOGGER >= ERROR */
		}
		m_bytesInBuffer = 0;
		m_buffer = null;
	}

	/**
	 * Buffers given data in secondary log buffer or writes it in secondary log if buffer
	 * contains enough data
	 *
	 * @param p_buffer           buffer with data to append
	 * @param p_bufferOffset     offset in buffer
	 * @param p_entryOrRangeSize size of the log entry/range
	 * @return whether the buffer was flushed or not
	 * @throws IOException          if the secondary log could not be written or buffer be read
	 * @throws InterruptedException if the caller was interrupted
	 */
	public boolean bufferData(final byte[] p_buffer, final int p_bufferOffset, final int p_entryOrRangeSize)
			throws IOException, InterruptedException {
		boolean ret = false;
		byte[] buffer;

		// Trim log entries (removes all NodeIDs)
		buffer = processBuffer(p_buffer, p_bufferOffset, p_entryOrRangeSize);
		if (m_bytesInBuffer + buffer.length >= m_buffer.length) {
			// Merge current secondary log buffer and new buffer and write to secondary log

			flushAllDataToSecLog(buffer, p_bufferOffset, buffer.length);
			ret = true;
		} else {
			// Append buffer to secondary log buffer
			System.arraycopy(buffer, 0, m_buffer, m_bytesInBuffer, buffer.length);
			m_bytesInBuffer += buffer.length;
		}

		return ret;
	}

	/**
	 * Changes log entries for storing in secondary log
	 *
	 * @param p_buffer           the log entries
	 * @param p_bufferOffset     offset in buffer
	 * @param p_entryOrRangeSize size of the log entry/range
	 * @return processed buffer
	 */
	private byte[] processBuffer(final byte[] p_buffer, final int p_bufferOffset, final int p_entryOrRangeSize) {
		byte[] buffer;
		int oldBufferOffset = p_bufferOffset;
		int newBufferOffset = 0;
		int logEntrySize;
		AbstractLogEntryHeader logEntryHeader;

		buffer = new byte[p_entryOrRangeSize];
		while (oldBufferOffset < p_bufferOffset + p_entryOrRangeSize) {
			// Determine header of next log entry
			logEntryHeader = AbstractLogEntryHeader.getPrimaryHeader(p_buffer, oldBufferOffset);
			logEntrySize = logEntryHeader.getHeaderSize(p_buffer, oldBufferOffset) + logEntryHeader
					.getLength(p_buffer, oldBufferOffset);

			// Copy primary log header, but skip NodeID and RangeID
			newBufferOffset += AbstractLogEntryHeader
					.convertAndPut(p_buffer, oldBufferOffset, buffer, newBufferOffset, logEntrySize,
							p_buffer.length - oldBufferOffset, logEntryHeader.getConversionOffset());
			oldBufferOffset += logEntrySize;
		}
		buffer = Arrays.copyOf(buffer, newBufferOffset);

		return buffer;
	}

	/**
	 * Flushes all data in secondary log buffer to secondary log regardless of the size
	 * Appends given data to buffer data and writes all data at once
	 *
	 * @param p_buffer           buffer with data to append
	 * @param p_bufferOffset     offset in buffer
	 * @param p_entryOrRangeSize size of the log entry/range
	 * @throws IOException          if the secondary log could not be written or buffer be read
	 * @throws InterruptedException if the caller was interrupted
	 */
	public void flushAllDataToSecLog(final byte[] p_buffer, final int p_bufferOffset, final int p_entryOrRangeSize)
			throws IOException, InterruptedException {
		byte[] dataToWrite;

		if (isBufferEmpty()) {
			// No data in secondary log buffer -> Write directly in secondary log
			m_secondaryLog.appendData(p_buffer, p_bufferOffset, p_entryOrRangeSize);
		} else {
			// There is data in secondary log buffer
			if (m_bytesInBuffer + p_entryOrRangeSize <= m_logSegmentSize) {
				// Data combined fits in one segment -> Flush buffer and write new data in secondary log with one access
				dataToWrite = new byte[m_bytesInBuffer + p_entryOrRangeSize];
				System.arraycopy(m_buffer, 0, dataToWrite, 0, m_bytesInBuffer);
				System.arraycopy(p_buffer, 0, dataToWrite, m_bytesInBuffer, p_entryOrRangeSize);

				m_secondaryLog.appendData(dataToWrite, 0, dataToWrite.length);
				m_bytesInBuffer = 0;
			} else {
				// Write buffer first
				m_secondaryLog.appendData(m_buffer, 0, m_bytesInBuffer);
				m_bytesInBuffer = 0;

				// Write new data
				m_secondaryLog.appendData(p_buffer, p_bufferOffset, p_entryOrRangeSize);
			}
		}
	}

	/**
	 * Flushes all data in secondary log buffer to secondary log regardless of the size
	 *
	 * @throws IOException          if the secondary log could not be written or buffer be read
	 * @throws InterruptedException if the caller was interrupted
	 */
	public void flushSecLogBuffer() throws IOException, InterruptedException {

		if (m_bytesInBuffer > 0) {
			m_secondaryLog.appendData(m_buffer, 0, m_bytesInBuffer);
			m_bytesInBuffer = 0;
		}
	}
}
