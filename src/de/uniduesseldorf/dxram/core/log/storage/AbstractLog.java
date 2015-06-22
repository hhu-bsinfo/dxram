
package de.uniduesseldorf.dxram.core.log.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReentrantLock;

import de.uniduesseldorf.dxram.core.log.LogHandler;

/**
 * Skeleton for a log
 * @author Kevin Beineke
 *         06.06.2014
 */
public abstract class AbstractLog {

	// Constants

	// Attributes
	// m_logFileSize must be a multiple of a flash page!
	private final long m_logFileSize;
	private long m_totalUsableSpace;
	private int m_logFileHeaderSize;
	private volatile long m_readPos;
	private volatile long m_writePos;
	private volatile long m_reorgPos;
	private volatile long m_bytesInRAF;
	private File m_logFile;
	private RandomAccessFile m_logRAF;
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
	public AbstractLog(final File p_logFile, final long p_logSize,
			final int p_logHeaderSize) {
		m_logFile = p_logFile;
		m_logFileSize = p_logSize + p_logHeaderSize;
		m_logFileHeaderSize = p_logHeaderSize;
		m_totalUsableSpace = p_logSize;

		m_readPos = 0;
		m_writePos = 0;
		m_reorgPos = 0;
		m_bytesInRAF = 0;
		m_logRAF = null;

		m_lock = new ReentrantLock(false);
	}

	// Getter
	/**
	 * Returns the total usable space
	 * @return the total usable space
	 */
	protected final long getTotalUsableSpace() {
		return m_totalUsableSpace;
	}

	/**
	 * Returns the number of bytes in log
	 * @return the number of bytes in log
	 */
	public final long getOccupiedSpace() {
		return m_bytesInRAF;
	}

	/**
	 * Determines the number of bytes between read and reorganization pointer
	 * @return the number of bytes between read and reorganization pointer
	 */
	protected final long getFreeSpaceBetweenReadAndReorgPos() {
		long ret;

		if (m_reorgPos >= m_readPos) {
			ret = m_reorgPos - m_readPos;
		} else {
			ret = m_totalUsableSpace - m_readPos + m_reorgPos;
		}
		return ret;
	}

	/**
	 * Determines the number of bytes left to write
	 * @return the number of bytes left to write
	 */
	protected final long getWritableSpace() {
		return m_totalUsableSpace - m_bytesInRAF;
	}

	// Setter
	/**
	 * Calculates and sets the read pointer
	 * @param p_readBytes
	 *            the number of read bytes
	 */
	protected final void calcAndSetReadPos(final int p_readBytes) {
		m_readPos = (m_readPos + p_readBytes) % m_totalUsableSpace;
		m_bytesInRAF = Math.max(0, m_bytesInRAF - p_readBytes);
	}

	/**
	 * Calculates and sets the write pointer
	 */
	protected final void calcAndSetWritePos() {
		m_writePos = (m_readPos + m_bytesInRAF) % m_totalUsableSpace;
	}

	// Methods
	/**
	 * Closes log
	 * @throws IOException
	 *             if the buffers could not be flushed
	 */
	public final void closeRing() throws IOException {
		m_bytesInRAF = 0;
		m_readPos = 0;
		m_writePos = 0;
		m_reorgPos = 0;
		m_logRAF.close();
	}

	/**
	 * Reset log
	 */
	public final void resetLog() {
		m_bytesInRAF = 0;
		m_readPos = 0;
		m_writePos = 0;
		m_reorgPos = 0;
	}

	/**
	 * Creates and initializes random access file
	 * @throws IOException
	 *             if the header could not be read or written
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 */
	protected final void createLogAndWriteHeader() throws IOException, InterruptedException {

		if (m_logFile.exists()) {
			m_logFile.delete();
		}
		// Create folders
		m_logFile.getParentFile().mkdirs();
		// Create file
		m_logFile.createNewFile();

		// Write header
		m_logRAF = openRingFile(m_logFile);
		m_logRAF.seek(0);
		if (PrimaryLog.class.isInstance(this)) {
			m_logRAF.write(LogHandler.PRIMLOG_MAGIC);
		} else {
			m_logRAF.write(LogHandler.SECLOG_MAGIC);
		}

		m_logRAF.seek(0);
		m_logRAF.setLength(m_logFileSize);
	}

	/**
	 * Opens a random access file for the log
	 * @param p_logFile
	 *            the log file
	 * @throws IOException
	 *             if opening the random access file failed
	 * @return file descriptor to the log file
	 */
	protected final RandomAccessFile openRingFile(final File p_logFile) throws IOException {
		return new RandomAccessFile(p_logFile, "rw");
	}

	/**
	 * Returns all data
	 * @param p_accessed
	 *            whether the RAF is accessed by another thread or not
	 * @throws IOException
	 *             if reading the random access file failed
	 * @return all data
	 */
	public final byte[][] readAll(final boolean p_accessed) throws IOException {
		byte[][] result = null;
		int i = 0;

		if (m_bytesInRAF > 0) {
			result = new byte[(int) Math.ceil((double) m_bytesInRAF / Integer.MAX_VALUE)][];
			while (m_bytesInRAF > 0) {
				if (m_bytesInRAF > Integer.MAX_VALUE) {
					result[i] = new byte[Integer.MAX_VALUE];
					readOnRAFRing(result[i], Integer.MAX_VALUE, true, p_accessed);
					m_bytesInRAF -= Integer.MAX_VALUE;
				} else {
					result[i] = new byte[(int) m_bytesInRAF];
					readOnRAFRing(result[i], (int) m_bytesInRAF, true, p_accessed);
					m_bytesInRAF = 0;
				}
				i++;
			}
		}
		return result;
	}

	/**
	 * Returns all data without manipulating the read pointer (read data is still valid)
	 * @param p_accessed
	 *            whether the RAF is accessed by another thread or not
	 * @throws IOException
	 *             if reading the random access file failed
	 * @return all data
	 */
	public final byte[][] readAllWithoutReadPtrSet(final boolean p_accessed) throws IOException {
		byte[][] result = null;
		int i = 0;
		long size = m_bytesInRAF;
		final long readPos = m_readPos;

		if (size > 0) {
			result = new byte[(int) Math.ceil((double) size / Integer.MAX_VALUE)][];
			while (size > 0) {
				if (size > Integer.MAX_VALUE) {
					result[i] = new byte[Integer.MAX_VALUE];
					readOnRAFRing(result[i], Integer.MAX_VALUE, false, p_accessed);
					size -= Integer.MAX_VALUE;
				} else {
					result[i] = new byte[(int) size];
					readOnRAFRing(result[i], (int) size, false, p_accessed);
					size = 0;
				}
				i++;
			}
		}
		m_readPos = readPos;
		return result;
	}

	/**
	 * Key function to read from log sequentially
	 * @param p_data
	 *            buffer to fill with log data
	 * @param p_length
	 *            number of bytes to read
	 * @param p_manipulateReadPtr
	 *            whether the read pointer is moved forward or not after reading
	 * @param p_accessed
	 *            whether the RAF is accessed by another thread or not
	 * @throws IOException
	 *             if reading the random access file failed
	 * @return number of bytes that were read successfully
	 */
	protected final int readOnRAFRing(final byte[] p_data, final int p_length,
			final boolean p_manipulateReadPtr, final boolean p_accessed) throws IOException {
		final long bytesUntilEnd = m_totalUsableSpace - m_readPos;

		if (p_length > 0) {
			if (p_accessed) {
				m_lock.lock();
			}
			m_logRAF.seek(m_logFileHeaderSize + m_readPos);
			if (p_length <= bytesUntilEnd) {
				m_logRAF.readFully(p_data, 0, p_length);
			} else {
				// Twofold cyclic read access
				// NOTE: bytesUntilEnd is smaller than p_length -> smaller than Integer.MAX_VALUE
				m_logRAF.readFully(p_data, 0, (int) bytesUntilEnd);
				m_logRAF.seek(m_logFileHeaderSize);
				m_logRAF.readFully(p_data, (int) bytesUntilEnd, p_length - (int) bytesUntilEnd);
			}
			if (p_manipulateReadPtr) {
				calcAndSetReadPos(p_length);
			}
			if (p_accessed) {
				m_lock.unlock();
			}
		}
		return p_length;
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
	protected final void readOnRAFRingRandomly(final byte[] p_data, final int p_length,
			final long p_readPos, final boolean p_accessed) throws IOException {
		final long innerLogSeekPos = m_logFileHeaderSize + p_readPos;
		final long bytesUntilEnd = m_totalUsableSpace - p_readPos;

		if (p_length > 0) {
			if (p_accessed) {
				m_lock.lock();
			}
			m_logRAF.seek(innerLogSeekPos);
			if (p_length <= bytesUntilEnd) {
				m_logRAF.readFully(p_data, 0, p_length);
			} else {
				// Twofold cyclic read access
				// NOTE: bytesUntilEnd is smaller than p_length -> smaller than Integer.MAX_VALUE
				m_logRAF.readFully(p_data, 0, (int) bytesUntilEnd);
				m_logRAF.seek(m_logFileHeaderSize);
				m_logRAF.readFully(p_data, (int) bytesUntilEnd, p_length - (int) bytesUntilEnd);
			}
			if (p_accessed) {
				m_lock.unlock();
			}
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
	 * @param p_accessed
	 *            whether the RAF is accessed by another thread or not
	 * @throws IOException
	 *             if reading the random access file failed
	 * @return number of bytes that were written successfully
	 */
	protected final int appendToLog(final byte[] p_data, final int p_bufferOffset,
			final int p_length, final boolean p_accessed) throws IOException {
		final long bytesUntilEnd;
		final long freeSpace = getWritableSpace();
		final int writableBytes = (int) Math.min(p_length, freeSpace);

		if (p_length > 0) {
			if (p_accessed) {
				m_lock.lock();
			}
			if (m_writePos >= m_readPos) {
				bytesUntilEnd = m_totalUsableSpace - m_writePos;
			} else {
				bytesUntilEnd = m_readPos - m_writePos;
			}

			calcAndSetWritePos();
			m_logRAF.seek(m_logFileHeaderSize + m_writePos);
			if (writableBytes <= bytesUntilEnd) {
				m_logRAF.write(p_data, p_bufferOffset, writableBytes);
			} else {
				// Twofold cyclic write access
				// NOTE: bytesUntilEnd is smaller than p_length -> smaller than Integer.MAX_VALUE
				m_logRAF.write(p_data, p_bufferOffset, (int) bytesUntilEnd);
				m_logRAF.seek(m_logFileHeaderSize);
				m_logRAF.write(p_data, p_bufferOffset + (int) bytesUntilEnd, writableBytes - (int) bytesUntilEnd);
			}
			// m_logRAF.getFD().sync();
			m_bytesInRAF += writableBytes;
			if (p_accessed) {
				m_lock.unlock();
			}
		}
		return writableBytes;
	}

	/**
	 * Key function to write in log sequentially by reorganization thread without manipulation write pointer
	 * @param p_data
	 *            buffer with data to write in log
	 * @param p_offset
	 *            offset in log file
	 * @param p_length
	 *            number of bytes to write
	 * @param p_innerWritePos
	 *            the position within the log file
	 * @param p_accessed
	 *            whether the RAF is accessed by another thread or not
	 * @throws IOException
	 *             if reading the random access file failed
	 * @return number of bytes that were written successfully
	 */
	protected final int appendToLogWithoutPtrManipulation(final byte[] p_data, final int p_offset,
			final int p_length, final long p_innerWritePos, final boolean p_accessed) throws IOException {
		final long bytesUntilEnd;
		final long freeSpace = getFreeSpaceBetweenReadAndReorgPos();
		final int writableData = (int) Math.min(p_length, freeSpace);

		if (m_reorgPos >= p_innerWritePos) {
			bytesUntilEnd = m_reorgPos - p_innerWritePos;
		} else {
			bytesUntilEnd = m_totalUsableSpace - p_innerWritePos;
		}

		if (p_length > 0) {
			if (p_accessed) {
				m_lock.lock();
			}
			m_logRAF.seek(m_logFileHeaderSize + p_innerWritePos);
			if (writableData <= bytesUntilEnd) {
				m_logRAF.write(p_data, p_offset, writableData);
			} else {
				// Twofold cyclic write access
				// NOTE: bytesUntilEnd is smaller than p_length -> smaller than Integer.MAX_VALUE
				m_logRAF.write(p_data, p_offset, (int) bytesUntilEnd);
				m_logRAF.seek(m_logFileHeaderSize);
				m_logRAF.write(p_data, p_offset + (int) bytesUntilEnd, writableData - (int) bytesUntilEnd);
			}
			// m_logRAF.getFD().sync();
			if (p_accessed) {
				m_lock.unlock();
			}
		}
		return writableData;
	}

	/**
	 * Key function to write in log
	 * @param p_data
	 *            buffer with data to write in log
	 * @param p_bufferOffset
	 *            offset in buffer
	 * @param p_logOffset
	 *            offset in log file
	 * @param p_length
	 *            number of bytes to write
	 * @param p_accessed
	 *            whether the RAF is accessed by another thread or not
	 * @throws IOException
	 *             if reading the random access file failed
	 * @return number of bytes that were written successfully
	 */
	protected final int writeToLog(final byte[] p_data, final int p_bufferOffset, final long p_logOffset,
			final int p_length, final boolean p_accessed) throws IOException {
		final long bytesUntilEnd;

		if (p_length > 0) {
			if (p_accessed) {
				m_lock.lock();
			}
			m_logRAF.seek(m_logFileHeaderSize + p_logOffset);
			if (p_logOffset + p_length <= m_totalUsableSpace) {
				m_logRAF.write(p_data, p_bufferOffset, p_length);
			} else {
				// Twofold cyclic write access
				// NOTE: bytesUntilEnd is smaller than p_length -> smaller than Integer.MAX_VALUE
				bytesUntilEnd = m_totalUsableSpace - p_logOffset;
				m_logRAF.write(p_data, p_bufferOffset, (int) bytesUntilEnd);
				m_logRAF.seek(m_logFileHeaderSize);
				m_logRAF.write(p_data, p_bufferOffset + (int) bytesUntilEnd, p_length - (int) bytesUntilEnd);
			}
			m_bytesInRAF += p_length;
			if (p_accessed) {
				m_lock.unlock();
			}
		}
		return p_length;
	}

	/**
	 * Key function to write in log
	 * @param p_data
	 *            buffer with data to write in log
	 * @param p_bufferOffset
	 *            offset in buffer
	 * @param p_logOffset
	 *            offset in log file
	 * @param p_length
	 *            number of bytes to write
	 * @param p_accessed
	 *            whether the RAF is accessed by another thread or not
	 * @throws IOException
	 *             if reading the random access file failed
	 * @return number of bytes that were written successfully
	 */
	protected final int overwriteLog(final byte[] p_data, final int p_bufferOffset, final long p_logOffset,
			final int p_length, final boolean p_accessed) throws IOException {
		final long bytesUntilEnd;

		if (p_length > 0) {
			if (p_accessed) {
				m_lock.lock();
			}
			m_logRAF.seek(m_logFileHeaderSize + p_logOffset);
			if (p_logOffset + p_length <= m_totalUsableSpace) {
				m_logRAF.write(p_data, p_bufferOffset, p_length);
			} else {
				// Twofold cyclic write access
				// NOTE: bytesUntilEnd is smaller than p_length -> smaller than Integer.MAX_VALUE
				bytesUntilEnd = m_totalUsableSpace - p_logOffset;
				m_logRAF.write(p_data, p_bufferOffset, (int) bytesUntilEnd);
				m_logRAF.seek(m_logFileHeaderSize);
				m_logRAF.write(p_data, p_bufferOffset + (int) bytesUntilEnd, p_length - (int) bytesUntilEnd);
			}
			if (p_accessed) {
				m_lock.unlock();
			}
		}
		return p_length;
	}

	/**
	 * Updates byte counter
	 * @param p_length
	 *            number of deleted bytes
	 */
	protected final void removeFromLog(final int p_length) {
		m_bytesInRAF -= p_length;
	}

	/**
	 * Clears all log data by manipulating the pointers
	 * @note is called after flushing
	 */
	protected final void clearAllLogData() {
		m_bytesInRAF = 0;
		m_readPos = m_writePos;
	}

	/**
	 * Deletes the log file
	 * @throws IOException
	 *             if deleting the random access file failed
	 * @return whether the deletion was successful or not
	 */
	protected final boolean deleteLogFile() throws IOException {
		return m_logFile.delete();
	}
}
