
package de.uniduesseldorf.dxram.core.log.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Skeleton for a log
 * @author Kevin Beineke
 *         06.06.2014
 */
public abstract class AbstractLog {

	// Attributes
	private final long m_logFileSize;
	private long m_totalUsableSpace;
	private int m_logFileHeaderSize;
	private File m_logFile;
	private RandomAccessFile m_randomAccessFile;
	private ReentrantLock m_lock;

	// Constructors
	/**
	 * Initializes the common resources of a log
	 * @param p_logFile
	 *            the random access file (RAF) of the log
	 * @param p_logSize
	 *            the size in byte of the log
	 * @param p_logHeaderSize
	 *            the size in byte of the log header
	 */
	public AbstractLog(final File p_logFile, final long p_logSize, final int p_logHeaderSize) {
		m_logFile = p_logFile;
		m_logFileSize = p_logSize + p_logHeaderSize;
		m_logFileHeaderSize = p_logHeaderSize;
		m_totalUsableSpace = p_logSize;

		m_randomAccessFile = null;

		m_lock = new ReentrantLock(false);
	}

	/**
	 * Writes data to log
	 * @param p_data
	 *            a buffer
	 * @param p_offset
	 *            offset within the buffer
	 * @param p_length
	 *            length of data
	 * @throws InterruptedException
	 *             if the write access fails
	 * @throws IOException
	 *             if the write access fails
	 * @return number of successfully written bytes
	 */
	abstract int appendData(final byte[] p_data, final int p_offset, final int p_length) throws IOException,
	InterruptedException;

	/**
	 * Returns the number of bytes in log
	 * @return the number of bytes in log
	 */
	abstract long getOccupiedSpace();

	// Methods
	/**
	 * Opens a random access file for the log
	 * @param p_logFile
	 *            the log file
	 * @throws IOException
	 *             if opening the random access file failed
	 * @return file descriptor to the log file
	 */
	private RandomAccessFile openLog(final File p_logFile) throws IOException {
		return new RandomAccessFile(p_logFile, "rw");
	}

	/**
	 * Creates and initializes random access file
	 * @param p_header
	 *            the log type specific header
	 * @throws IOException
	 *             if the header could not be read or written
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 */
	protected final void createLogAndWriteHeader(final byte[] p_header) throws IOException, InterruptedException {

		if (m_logFile.exists()) {
			m_logFile.delete();
		}
		// Create folders
		m_logFile.getParentFile().mkdirs();
		// Create file
		m_logFile.createNewFile();

		// Write header
		m_randomAccessFile = openLog(m_logFile);
		m_randomAccessFile.seek(0);
		m_randomAccessFile.write(p_header);

		m_randomAccessFile.seek(0);
		m_randomAccessFile.setLength(m_logFileSize);
	}

	/**
	 * Closes the log
	 * @throws InterruptedException
	 *             if the closure fails
	 * @throws IOException
	 *             if the flushing during closure fails
	 */
	public final void closeLog() throws IOException {
		m_randomAccessFile.close();
	}

	/**
	 * Key function to read from log randomly
	 * @param p_data
	 *            buffer to fill with log data
	 * @param p_length
	 *            number of bytes to read
	 * @param p_readPos
	 *            the position within the log file
	 * @param p_accessed
	 *            whether the RAF is accessed by another thread or not
	 * @throws IOException
	 *             if reading the random access file failed
	 */
	protected final void readFromSecondaryLog(final byte[] p_data, final int p_length, final long p_readPos, final boolean p_accessed) throws IOException {
		final long innerLogSeekPos = m_logFileHeaderSize + p_readPos;
		final long bytesUntilEnd = m_totalUsableSpace - p_readPos;

		if (p_length > 0) {
			if (p_accessed) {
				m_lock.lock();
			}

			assert p_length <= bytesUntilEnd;

			m_randomAccessFile.seek(innerLogSeekPos);
			m_randomAccessFile.readFully(p_data, 0, p_length);

			if (p_accessed) {
				m_lock.unlock();
			}
		}
	}

	/**
	 * Key function to read from log file randomly
	 * @param p_data
	 *            buffer to fill with log data
	 * @param p_length
	 *            number of bytes to read
	 * @param p_readPos
	 *            the position within the log file
	 * @param p_randomAccessFile
	 *            the RandomAccessFile
	 * @param p_headerSize
	 *            the length of the secondary log header
	 * @throws IOException
	 *             if reading the random access file failed
	 */
	protected static void readFromSecondaryLogFile(final byte[] p_data, final int p_length,
			final long p_readPos, final RandomAccessFile p_randomAccessFile, final short p_headerSize) throws IOException {
		final long innerLogSeekPos = p_headerSize + p_readPos;
		final long bytesUntilEnd = p_randomAccessFile.length() - (p_headerSize + p_readPos);

		if (p_length > 0) {
			assert p_length <= bytesUntilEnd;

			p_randomAccessFile.seek(innerLogSeekPos);
			p_randomAccessFile.readFully(p_data, 0, p_length);
		}
	}

	/**
	 * Key function to write in log sequentially
	 * @param p_data
	 *            buffer with data to write in log
	 * @param p_bufferOffset
	 *            offset in buffer
	 * @param p_length
	 *            number of bytes to write
	 * @param p_writePos
	 *            the current write position
	 * @throws IOException
	 *             if reading the random access file failed
	 * @return updated write position
	 */
	protected final long appendToPrimaryLog(final byte[] p_data, final int p_bufferOffset, final int p_length, final long p_writePos) throws IOException {
		long ret;
		final long bytesUntilEnd;

		// System.out.println("PRIMLOG-Writing: " + p_length);
		if (p_writePos + p_length <= m_totalUsableSpace) {
			m_randomAccessFile.seek(m_logFileHeaderSize + p_writePos);
			m_randomAccessFile.write(p_data, p_bufferOffset, p_length);
			ret = p_writePos + p_length;
		} else {
			// Twofold cyclic write access
			// NOTE: bytesUntilEnd is smaller than p_length -> smaller than Integer.MAX_VALUE
			bytesUntilEnd = m_totalUsableSpace - p_writePos;
			m_randomAccessFile.seek(m_logFileHeaderSize + p_writePos);
			m_randomAccessFile.write(p_data, p_bufferOffset, (int) bytesUntilEnd);
			m_randomAccessFile.seek(m_logFileHeaderSize);
			m_randomAccessFile.write(p_data, p_bufferOffset + (int) bytesUntilEnd, p_length - (int) bytesUntilEnd);
			ret = p_length - bytesUntilEnd;
		}

		return ret;
	}

	/**
	 * Key function to write in log
	 * @param p_data
	 *            buffer with data to write in log
	 * @param p_bufferOffset
	 *            offset in buffer
	 * @param p_readPos
	 *            offset in log file
	 * @param p_length
	 *            number of bytes to write
	 * @param p_accessed
	 *            whether the RAF is accessed by another thread or not
	 * @throws IOException
	 *             if reading the random access file failed
	 * @return number of bytes that were written successfully
	 */
	protected final int writeToSecondaryLog(final byte[] p_data, final int p_bufferOffset, final long p_readPos, final int p_length, final boolean p_accessed)
			throws IOException {

		// System.out.println("SECLOG-Writing: " + p_length);
		if (p_length > 0) {
			if (p_accessed) {
				m_lock.lock();
			}

			assert p_readPos + p_length <= m_totalUsableSpace;

			m_randomAccessFile.seek(m_logFileHeaderSize + p_readPos);
			m_randomAccessFile.write(p_data, p_bufferOffset, p_length);

			if (p_accessed) {
				m_lock.unlock();
			}
		}
		return p_length;
	}
}
