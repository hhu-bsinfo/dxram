package de.uniduesseldorf.dxram.core.log.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
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
	private static Checksum m_crc;


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

		m_crc = new CRC32();
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
	protected final long getWritableSpace(){
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
	 *            if the buffers could not be flushed
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
	 *            if the header could not be read or written
	 * @throws InterruptedException
	 *            if the caller was interrupted
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
			// Write read, write and reorganization pointer at the beginning
			m_logRAF.writeLong(m_readPos);
			calcAndSetWritePos();
			m_logRAF.writeLong(m_writePos);
			m_logRAF.writeLong(m_reorgPos);
		}

		m_logRAF.seek(0);
		m_logRAF.setLength(m_logFileSize);
	}

	/**
	 * Opens a random access file for the log
	 * @param p_logFile
	 *            the log file
	 * @throws IOException
	 *            if opening the random access file failed
	 * @return file descriptor to the log file
	 */
	protected final RandomAccessFile openRingFile(final File p_logFile) throws IOException {
		return new RandomAccessFile(p_logFile, "rw");
	}

	/**
	 * Returns all data
	 * @throws IOException
	 *            if reading the random access file failed
	 * @return all data
	 */
	public final byte[][] readAll() throws IOException {
		byte[][] result = null;
		int i = 0;

		if (m_bytesInRAF > 0) {
			result = new byte[(int) Math.ceil((double) m_bytesInRAF / Integer.MAX_VALUE)][];
			while (m_bytesInRAF > 0) {
				if (m_bytesInRAF > Integer.MAX_VALUE) {
					result[i] = new byte[Integer.MAX_VALUE];
					readOnRAFRing(result[i], Integer.MAX_VALUE, true);
					m_bytesInRAF -= Integer.MAX_VALUE;
				} else {
					result[i] = new byte[(int) m_bytesInRAF];
					readOnRAFRing(result[i], (int) m_bytesInRAF, true);
					m_bytesInRAF = 0;
				}
				i++;
			}
		}
		return result;
	}

	/**
	 * Returns all data without manipulating the read pointer (read data is still valid)
	 * @throws IOException
	 *            if reading the random access file failed
	 * @return all data
	 */
	public final byte[][] readAllWithoutReadPtrSet() throws IOException {
		byte[][] result = null;
		int i = 0;
		long size = m_bytesInRAF;
		final long readPos = m_readPos;

		if (size > 0) {
			result = new byte[(int) Math.ceil((double) size / Integer.MAX_VALUE)][];
			while (size > 0) {
				if (size > Integer.MAX_VALUE) {
					result[i] = new byte[Integer.MAX_VALUE];
					readOnRAFRing(result[i], Integer.MAX_VALUE, false);
					size -= Integer.MAX_VALUE;
				} else {
					result[i] = new byte[(int) size];
					readOnRAFRing(result[i], (int) size, false);
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
	 * @throws IOException
	 *            if reading the random access file failed
	 * @return number of bytes that were read successfully
	 */
	protected final int readOnRAFRing(final byte[] p_data, final int p_length,
			final boolean p_manipulateReadPtr) throws IOException{
		final long bytesUntilEnd = m_totalUsableSpace - m_readPos;

		if (p_length > 0) {
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
	 * @throws IOException
	 *            if reading the random access file failed
	 */
	protected final void readOnRAFRingRandomly(final byte[] p_data, final int p_length,
			final long p_readPos) throws IOException {
		final long innerLogSeekPos = m_logFileHeaderSize + p_readPos;
		final long bytesUntilEnd = m_totalUsableSpace - p_readPos;

		if (p_length > 0) {
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
	 * @throws IOException
	 *            if reading the random access file failed
	 * @return number of bytes that were written successfully
	 */
	protected final int appendToLog(final byte[] p_data,
			final int p_bufferOffset, final int p_length) throws IOException{
		final long bytesUntilEnd;
		final long freeSpace = getWritableSpace();
		final int writableBytes = (int) Math.min(p_length, freeSpace);

		if (p_length > 0) {
			//System.out.println("Writing " + p_length + " bytes to " + super.getClass().getName());
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
			//m_logRAF.getFD().sync();
			m_bytesInRAF += writableBytes;
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
	 * @throws IOException
	 *            if reading the random access file failed
	 * @return number of bytes that were written successfully
	 */
	protected final int appendToLogWithoutPtrManipulation(final byte[] p_data, final int p_offset,
			final int p_length, final long p_innerWritePos) throws IOException{
		final long bytesUntilEnd;
		final long freeSpace = getFreeSpaceBetweenReadAndReorgPos();
		final int writableData = (int) Math.min(p_length, freeSpace);

		if (m_reorgPos >= p_innerWritePos) {
			bytesUntilEnd = m_reorgPos - p_innerWritePos;
		} else {
			bytesUntilEnd = m_totalUsableSpace - p_innerWritePos;
		}

		if (p_length > 0) {
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
			//m_logRAF.getFD().sync();
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
	 * @throws IOException
	 *            if reading the random access file failed
	 * @return number of bytes that were written successfully
	 */
	protected final int writeToLog(final byte[] p_data,
			final int p_bufferOffset, final long p_logOffset, final int p_length) throws IOException{
		final long bytesUntilEnd;

		if (p_length > 0) {
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
	 * @throws IOException
	 *            if reading the random access file failed
	 * @return number of bytes that were written successfully
	 */
	protected final int overwriteLog(final byte[] p_data,
			final int p_bufferOffset, final long p_logOffset, final int p_length) throws IOException{
		final long bytesUntilEnd;

		if (p_length > 0) {
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
	 *            if deleting the random access file failed
	 * @return whether the deletion was successful or not
	 */
	protected final boolean deleteLogFile() throws IOException {
		return m_logFile.delete();
	}

	/**
	 * Generates a log entry with filled-in header but without any payload
	 * @param p_chunk
	 *            the Chunk
	 * @return the log entry
	 */
	public static byte[] createPrimaryLogEntryHeader(final Chunk p_chunk) {
		byte[] result;
		long chunkID;

		result = new byte[LogHandler.PRIMARY_HEADER_SIZE];
		chunkID = p_chunk.getChunkID();
		putChunkIDInLogEntryHeader(result, ChunkID.getCreatorID(chunkID), ChunkID.getLocalID(chunkID));
		putLengthInLogEntryHeader(result, p_chunk.getSize());
		putVersionInLogEntryHeader(result, p_chunk.getVersion());
		//putChecksumInLogEntryHeader(result, calculateChecksumOfPayload(p_chunk.getData().array()));

		return result;
	}

	/**
	 * Generates a tombstone
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the tombstone
	 */
	public static byte[] createTombstone(final long p_chunkID) {
		byte[] result;

		result = new byte[LogHandler.PRIMARY_HEADER_SIZE];
		putChunkIDInLogEntryHeader(result, ChunkID.getCreatorID(p_chunkID), ChunkID.getLocalID(p_chunkID));
		putLengthInLogEntryHeader(result, 0);
		putVersionInLogEntryHeader(result, -1);
		//putChecksumInLogEntryHeader(result, calculateChecksumOfPayload(p_chunk.getData().array()));

		return result;
	}

	/**
	 * Marks the log entry as invalid
	 * @param p_buffer
	 *            the buffer
	 * @param p_offset
	 *            the offset in buffer
	 */
	public static void markLogEntryAsInvalid(final byte[] p_buffer, final int p_offset) {
		final byte invalid = (byte) 0xFF;
		int offset;

		// LID
		offset = p_offset;
		for (int i = 0; i < LogHandler.LOG_HEADER_LID_SIZE; i++) {
			p_buffer[offset + i] = invalid;
		}
	}

	/**
	 * Puts ChunkID in log entry header
	 * @param p_logEntry
	 *            log entry
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_lid
	 *            the LID
	 */
	public static void putChunkIDInLogEntryHeader(final byte[] p_logEntry, final short p_nodeID, final long p_lid) {
		// NodeID
		for (int i = 0; i < LogHandler.LOG_HEADER_NID_SIZE; i++) {
			p_logEntry[i] = (byte) ((p_nodeID >> (i * 8)) & 0xff);
		}
		// LID
		for (int i = 0; i < LogHandler.LOG_HEADER_LID_SIZE; i++) {
			p_logEntry[LogHandler.LOG_HEADER_NID_SIZE + i] = (byte) (p_lid >> (i * 8));
		}
	}

	/**
	 * Puts length of log entry in log entry header
	 * @param p_logEntry
	 *            log entry
	 * @param p_length
	 *            the length
	 */
	public static void putLengthInLogEntryHeader(final byte[] p_logEntry, final int p_length) {
		final int offset = LogHandler.PRIMARY_HEADER_LEN_OFFSET;

		for (int i = 0; i < LogHandler.LOG_HEADER_LEN_SIZE; i++) {
			p_logEntry[offset + i] = (byte) (p_length >> (i * 8));
		}
	}

	/**
	 * Puts version of log entry in log entry header
	 * @param p_logEntry
	 *            log entry
	 * @param p_version
	 *            the version
	 */
	public static void putVersionInLogEntryHeader(final byte[] p_logEntry, final int p_version) {
		final int offset = LogHandler.PRIMARY_HEADER_VER_OFFSET;

		for (int i = 0; i < LogHandler.LOG_HEADER_VER_SIZE; i++) {
			p_logEntry[offset + i] = (byte) (p_version >> (i * 8));
		}
	}

	/**
	 * Puts length of log entry in log entry header
	 * @param p_logEntry
	 *            log entry
	 * @param p_checksum
	 *            the checksum
	 */
	public static void putChecksumInLogEntryHeader(final byte[] p_logEntry, final long p_checksum) {
		final int offset = LogHandler.PRIMARY_HEADER_CRC_OFFSET;

		for (int i = 0; i < LogHandler.LOG_HEADER_CRC_SIZE; i++) {
			p_logEntry[offset + i] = (byte) ((p_checksum >> (i * 8)) & 0xff);
		}
	}

	/**
	 * Returns NodeID of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the NodeID
	 */
	public static short getNodeIDOfLogEntry(final byte[] p_buffer, final int p_offset) {
		return (short) ((p_buffer[p_offset] & 0xff) + ((p_buffer[p_offset + 1] & 0xff) << 8));
	}

	/**
	 * Returns the LID of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @param p_primary
	 *            whether this is a primary log entry or not
	 * @return the LID
	 */
	public static long getLIDOfLogEntry(final byte[] p_buffer, final int p_offset, final boolean p_primary) {
		int offset = p_offset;
		if (p_primary) {
			offset = p_offset + LogHandler.PRIMARY_HEADER_LID_OFFSET;
		}

		return (long)((p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
				+ ((p_buffer[offset + 2] & 0xff) << 16) + (((long) p_buffer[offset + 3] & 0xff) << 24)
				+ (((long) p_buffer[offset + 4] & 0xff) << 32) + (((long) p_buffer[offset + 5] & 0xff) << 40));
	}

	/**
	 * Returns the ChunkID of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the ChunkID
	 */
	public static long getChunkIDOfLogEntry(final byte[] p_buffer, final int p_offset) {
		return ((long) getNodeIDOfLogEntry(p_buffer, p_offset) << 48) + getLIDOfLogEntry(p_buffer, p_offset, true);
	}

	/**
	 * Returns length of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @param p_primary
	 *            whether this is a primary log entry or not
	 * @return the length
	 */
	public static int getLengthOfLogEntry(final byte[] p_buffer, final int p_offset, final boolean p_primary) {
		int offset = p_offset + LogHandler.PRIMARY_HEADER_LEN_OFFSET;
		if (!p_primary) {
			offset -= LogHandler.LOG_HEADER_NID_SIZE;
		}

		return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
				+ ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24);
	}

	/**
	 * Returns version of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @param p_primary
	 *            whether this is a primary log entry or not
	 * @return the version
	 */
	public static int getVersionOfLogEntry(final byte[] p_buffer, final int p_offset, final boolean p_primary) {
		int offset = p_offset + LogHandler.PRIMARY_HEADER_VER_OFFSET;
		if (!p_primary) {
			offset -= LogHandler.LOG_HEADER_NID_SIZE;
		}

		return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
				+ ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24);
	}

	/**
	 * Returns the checksum of a log entry's payload
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @param p_primary
	 *            whether this is a primary log entry or not
	 * @return the checksum
	 */
	public static long getChecksumOfPayload(final byte[] p_buffer, final int p_offset, final boolean p_primary) {
		int offset = p_offset + LogHandler.PRIMARY_HEADER_CRC_OFFSET;
		if (!p_primary) {
			offset -= LogHandler.LOG_HEADER_NID_SIZE;
		}

		return (long)((p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
				+ ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24)
				+ ((p_buffer[offset + 4] & 0xff) << 32) + ((p_buffer[offset + 5] & 0xff) << 40)
				+ ((p_buffer[offset + 6] & 0xff) << 48) + ((p_buffer[offset + 7] & 0xff) << 54));
	}

	/**
	 * Prints the log header
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @param p_primary
	 *            whether this is a primary log entry or not
	 */
	public static void printLogHeader(final byte[] p_buffer, final int p_offset, final boolean p_primary) {
		System.out.println("********************Log Header********************");
		if (p_primary) {
			System.out.println("* NodeID: " + getNodeIDOfLogEntry(p_buffer, p_offset));
		}
		System.out.println("* LocalID: " + getLIDOfLogEntry(p_buffer, p_offset, p_primary));
		System.out.println("* Length: " + getLengthOfLogEntry(p_buffer, p_offset, p_primary));
		System.out.println("* Version: " + getVersionOfLogEntry(p_buffer, p_offset, p_primary));
		System.out.println("* Checksum: " + getChecksumOfPayload(p_buffer, p_offset, p_primary));
		System.out.println("**************************************************");
	}

	/**
	 * Calculates the CRC32 checksum of a log entry's payload
	 * @param p_payload
	 *            the payload
	 * @return the checksum
	 */
	public static long calculateChecksumOfPayload(final byte[] p_payload){

		m_crc.reset();
		m_crc.update(p_payload, 0, p_payload.length);

		return m_crc.getValue();
	}
}
