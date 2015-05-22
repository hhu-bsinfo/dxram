
package de.uniduesseldorf.dxram.core.log.storage;

import java.io.IOException;
import java.util.Arrays;

import de.uniduesseldorf.dxram.core.log.LogHandler;

/**
 * This class implements the secondary log buffer
 * @author Kevin Beineke
 *         20.06.2014
 */
public final class SecondaryLogBuffer {

	// Constants

	// Attributes
	private byte[] m_buffer;
	private int m_bytesInBuffer;

	private SecondaryLogWithSegments m_secondaryLog;

	// Constructors
	/**
	 * Creates an instance of SecondaryLogBuffer
	 * @param p_secondaryLog
	 *            Instance of the corresponding secondary log. Used to write directly to secondary
	 */
	public SecondaryLogBuffer(final SecondaryLogWithSegments p_secondaryLog) {

		m_secondaryLog = p_secondaryLog;

		m_bytesInBuffer = 0;
		m_buffer = new byte[LogHandler.FLASHPAGE_SIZE];
	}

	// Getter
	/**
	 * Returns whether the secondary log buffer is empty or not
	 * @return whether buffer is empty or not
	 */
	public boolean isBufferEmpty() {
		return m_bytesInBuffer == 0;
	}

	/**
	 * Returns the number of bytes
	 * @return the number of bytes
	 */
	public int getOccupiedSpace() {
		return m_bytesInBuffer;
	}

	// Methods
	/**
	 * Closes the buffer
	 */
	public void close() {
		try {
			flushSecLogBuffer();
		} catch (final IOException | InterruptedException e) {
			System.out.println("Could not flush secondary log buffer. Data loss possible!");
		}
		m_bytesInBuffer = 0;
		m_buffer = null;
	}

	/**
	 * Buffers given data in secondary log buffer or writes it in secondary log if buffer
	 * contains enough data
	 * @param p_buffer
	 *            buffer with data to append
	 * @param p_bufferOffset
	 *            offset in buffer
	 * @param p_entryOrRangeSize
	 *            size of the log entry/range
	 * @throws IOException
	 *             if the secondary log could not be written or buffer be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 */
	public void bufferData(final byte[] p_buffer, final int p_bufferOffset,
			final int p_entryOrRangeSize) throws IOException, InterruptedException {
		byte[] buffer;

		// Trim log entries (removes all NodeIDs)
		buffer = processBuffer(p_buffer, p_bufferOffset, p_entryOrRangeSize);
		if (m_bytesInBuffer + buffer.length >= LogHandler.FLASHPAGE_SIZE) {
			// Merge current secondary log buffer and new buffer and write to secondary log
			flushAllDataToSecLog(buffer, p_bufferOffset, buffer.length);
		} else {
			// Append buffer to secondary log buffer
			System.arraycopy(buffer, 0, m_buffer, m_bytesInBuffer, buffer.length);
			m_bytesInBuffer += buffer.length;
		}
	}

	/**
	 * Changes log entries for storing in secondary log
	 * @param p_buffer
	 *            the log entries
	 * @param p_bufferOffset
	 *            offset in buffer
	 * @param p_entryOrRangeSize
	 *            size of the log entry/range
	 * @return processed buffer
	 */
	private byte[] processBuffer(final byte[] p_buffer, final int p_bufferOffset, final int p_entryOrRangeSize) {
		byte[] buffer;
		int oldBufferOffset = p_bufferOffset;
		int newBufferOffset = 0;
		int logEntrySize;
		final int nidSize = LogHandler.LOG_HEADER_NID_SIZE;

		buffer = new byte[p_entryOrRangeSize];
		while (oldBufferOffset < p_bufferOffset + p_entryOrRangeSize) {
			// Determine header of next log entry
			logEntrySize = LogHandler.PRIMARY_HEADER_SIZE
					+ SecondaryLog.getLengthOfLogEntry(p_buffer, oldBufferOffset, true);
			System.arraycopy(p_buffer, oldBufferOffset + nidSize, buffer, newBufferOffset, logEntrySize - nidSize);

			oldBufferOffset += logEntrySize;
			newBufferOffset += logEntrySize - nidSize;
		}
		buffer = Arrays.copyOf(buffer, newBufferOffset);

		return buffer;
	}

	/**
	 * Flushes all data in secondary log buffer to secondary log regardless of the size
	 * Appends given data to buffer data and writes all data at once
	 * @param p_buffer
	 *            buffer with data to append
	 * @param p_bufferOffset
	 *            offset in buffer
	 * @param p_entryOrRangeSize
	 *            size of the log entry/range
	 * @throws IOException
	 *             if the secondary log could not be written or buffer be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 */
	public void flushAllDataToSecLog(final byte[] p_buffer, final int p_bufferOffset,
			final int p_entryOrRangeSize) throws IOException, InterruptedException {
		byte[] dataToWrite;

		if (isBufferEmpty()) {
			// No data in secondary log buffer -> Write directly in secondary log
			m_secondaryLog.appendData(p_buffer, p_bufferOffset, p_entryOrRangeSize, null);
		} else {
			// Data in secondary log buffer -> Flush buffer and write new data in secondary log with one access
			if (m_bytesInBuffer > 0) {
				dataToWrite = new byte[m_bytesInBuffer + p_entryOrRangeSize];
				System.arraycopy(m_buffer, 0, dataToWrite, 0, m_bytesInBuffer);
				System.arraycopy(p_buffer, 0, dataToWrite, m_bytesInBuffer, p_entryOrRangeSize);

				m_secondaryLog.appendData(dataToWrite, 0, dataToWrite.length, null);
				m_bytesInBuffer = 0;
			}
		}
	}

	/**
	 * Flushes all data in secondary log buffer to secondary log regardless of the size
	 * @throws IOException
	 *             if the secondary log could not be written or buffer be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 */
	public void flushSecLogBuffer() throws IOException, InterruptedException {

		if (m_bytesInBuffer > 0) {
			m_secondaryLog.appendData(m_buffer, 0, m_bytesInBuffer, null);
			m_bytesInBuffer = 0;
		}
	}
}
