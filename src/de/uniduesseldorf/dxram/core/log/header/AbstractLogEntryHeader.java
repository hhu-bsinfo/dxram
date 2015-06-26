
package de.uniduesseldorf.dxram.core.log.header;

import java.util.zip.CRC32;
import java.util.zip.Checksum;

import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.log.LogHandler;

/**
 * A helper class for the LogEntryHeaderInterface.
 * Provides methods to detect the type of a log entry header or to convert one.
 * @author Kevin Beineke
 *         25.06.2015
 */
public abstract class AbstractLogEntryHeader implements LogEntryHeaderInterface {

	// Attributes
	private static Checksum m_crc = new CRC32();
	private static LogEntryHeaderInterface m_normalLogEntryHeader = new NormalLogEntryHeader();
	private static LogEntryHeaderInterface m_migrationLogEntryHeader = new MigrationLogEntryHeader();
	private static LogEntryHeaderInterface m_tombstone = new Tombstone();

	// Methods
	/**
	 * Converts a log entry header from PrimaryWriteBuffer/Primary Log to a secondary log entry header
	 * and copies the payload
	 * @param p_input
	 *            the input buffer
	 * @param p_inputOffset
	 *            the input buffer offset
	 * @param p_output
	 *            the output buffer
	 * @param p_outputOffset
	 *            the output buffer offset
	 * @param p_logEntrySize
	 *            the length of the log entry
	 * @param p_bytesUntilEnd
	 *            the number of bytes to the end of the input buffer
	 */
	public static void convertAndPut(final byte[] p_input, final int p_inputOffset, final byte[] p_output,
			final int p_outputOffset, final int p_logEntrySize, final int p_bytesUntilEnd) {
		LogEntryHeaderInterface type;
		short typeOffset;

		type = getType(p_input, p_inputOffset);
		typeOffset = type.getLIDOffset();

		if (p_bytesUntilEnd >= p_logEntrySize || p_bytesUntilEnd <= 0) {
			System.arraycopy(p_input, p_inputOffset + typeOffset, p_output,
					p_outputOffset, p_logEntrySize - typeOffset);
		} else {
			if (p_bytesUntilEnd > typeOffset) {
				System.arraycopy(p_input, p_inputOffset + typeOffset, p_output,
						p_outputOffset, p_bytesUntilEnd - typeOffset);
				System.arraycopy(p_input, 0, p_output, p_outputOffset
						+ p_bytesUntilEnd - typeOffset, p_logEntrySize
						- p_bytesUntilEnd);
			} else {
				System.arraycopy(p_input, 0, p_output, p_outputOffset
						+ typeOffset - p_bytesUntilEnd, p_logEntrySize
						- (typeOffset - p_bytesUntilEnd));
			}
		}
	}

	/**
	 * Marks the log entry as invalid
	 * @param p_buffer
	 *            the buffer
	 * @param p_offset
	 *            the offset in buffer
	 */
	public static void markAsInvalid(final byte[] p_buffer, final int p_offset) {
		final byte invalid = (byte) 0xFF;
		int offset;

		// LID
		offset = p_offset;
		for (int i = 0; i < LogHandler.LOG_ENTRY_LID_SIZE; i++) {
			p_buffer[offset + i] = invalid;
		}
	}

	/**
	 * Puts type of log entry in log entry header
	 * @param p_logEntry
	 *            log entry
	 * @param p_type
	 *            the type (0 => normal, 1 => migration)
	 * @param p_offset
	 *            the type-specific offset
	 */
	public static void putType(final byte[] p_logEntry, final byte p_type, final byte p_offset) {
		p_logEntry[p_offset] = p_type;
	}

	/**
	 * Puts ChunkID in log entry header
	 * @param p_logEntry
	 *            log entry
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_offset
	 *            the type-specific offset
	 */
	public static void putChunkID(final byte[] p_logEntry, final long p_chunkID, final byte p_offset) {
		// NodeID
		for (int i = 0; i < LogHandler.LOG_ENTRY_NID_SIZE; i++) {
			p_logEntry[p_offset + i] = (byte) (ChunkID.getCreatorID(p_chunkID) >> i * 8 & 0xff);
		}

		// LocalID
		for (int i = 0; i < LogHandler.LOG_ENTRY_LID_SIZE; i++) {
			p_logEntry[p_offset + LogHandler.LOG_ENTRY_NID_SIZE + i] = (byte) (p_chunkID >> i * 8 & 0xff);
		}
	}

	/**
	 * Puts length of log entry in log entry header
	 * @param p_logEntry
	 *            log entry
	 * @param p_length
	 *            the length
	 * @param p_offset
	 *            the type-specific offset
	 */
	public static void putLength(final byte[] p_logEntry, final int p_length, final byte p_offset) {
		for (int i = 0; i < LogHandler.LOG_ENTRY_LEN_SIZE; i++) {
			p_logEntry[p_offset + i] = (byte) (p_length >> i * 8 & 0xff);
		}
	}

	/**
	 * Puts version of log entry in log entry header
	 * @param p_logEntry
	 *            log entry
	 * @param p_version
	 *            the version
	 * @param p_offset
	 *            the type-specific offset
	 */
	public static void putVersion(final byte[] p_logEntry, final int p_version, final byte p_offset) {
		for (int i = 0; i < LogHandler.LOG_ENTRY_VER_SIZE; i++) {
			p_logEntry[p_offset + i] = (byte) (p_version >> i * 8 & 0xff);
		}
	}

	/**
	 * Puts length of log entry in log entry header
	 * @param p_logEntry
	 *            log entry
	 * @param p_checksum
	 *            the checksum
	 * @param p_offset
	 *            the type-specific offset
	 */
	public static void putChecksum(final byte[] p_logEntry, final long p_checksum, final byte p_offset) {
		for (int i = 0; i < LogHandler.LOG_ENTRY_CRC_SIZE; i++) {
			p_logEntry[p_offset + i] = (byte) (p_checksum >> i * 8 & 0xff);
		}
	}

	/**
	 * Puts RangeID of log entry in log entry header
	 * @param p_logEntry
	 *            log entry
	 * @param p_rangeID
	 *            the RangeID
	 * @param p_offset
	 *            the type-specific offset
	 */
	public static void putRangeID(final byte[] p_logEntry, final byte p_rangeID, final byte p_offset) {
		p_logEntry[p_offset] = p_rangeID;
	}

	/**
	 * Puts source of log entry in log entry header
	 * @param p_logEntry
	 *            log entry
	 * @param p_source
	 *            the source
	 * @param p_offset
	 *            the type-specific offset
	 */
	public static void putSource(final byte[] p_logEntry, final short p_source, final byte p_offset) {
		for (int i = 0; i < LogHandler.LOG_ENTRY_SRC_SIZE; i++) {
			p_logEntry[p_offset + i] = (byte) (p_source >> i * 8 & 0xff);
		}
	}

	/**
	 * Calculates the CRC32 checksum of a log entry's payload
	 * @param p_payload
	 *            the payload
	 * @return the checksum
	 */
	public static long calculateChecksumOfPayload(final byte[] p_payload) {

		m_crc.reset();
		m_crc.update(p_payload, 0, p_payload.length);

		return m_crc.getValue();
	}

	/**
	 * Returns type of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the version
	 */
	public static LogEntryHeaderInterface getType(final byte[] p_buffer, final int p_offset) {
		LogEntryHeaderInterface ret;

		if (p_buffer[0] == 0) {
			ret = m_normalLogEntryHeader;
		} else if (p_buffer[0] == 1) {
			ret = m_migrationLogEntryHeader;
		} else {
			ret = m_tombstone;
		}
		return ret;
	}
}
