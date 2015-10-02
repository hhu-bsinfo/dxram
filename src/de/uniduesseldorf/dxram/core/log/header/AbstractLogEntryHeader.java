
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
	private static final Checksum CRC = new CRC32();
	private static final LogEntryHeaderInterface DEFAULT_PRIM_LOG_ENTRY_HEADER = new DefaultPrimLogEntryHeader();
	private static final LogEntryHeaderInterface MIGRATION_PRIM_LOG_ENTRY_HEADER = new MigrationPrimLogEntryHeader();
	private static final LogEntryHeaderInterface DEFAULT_PRIM_LOG_TOMBSTONE = new DefaultPrimLogTombstone();
	private static final LogEntryHeaderInterface MIGRATION_PRIM_LOG_TOMBSTONE = new MigrationPrimLogTombstone();

	private static final LogEntryHeaderInterface DEFAULT_SEC_LOG_ENTRY_HEADER = new DefaultSecLogEntryHeader();
	private static final LogEntryHeaderInterface MIGRATION_SEC_LOG_ENTRY_HEADER = new MigrationSecLogEntryHeader();
	private static final LogEntryHeaderInterface DEFAULT_SEC_LOG_TOMBSTONE = new DefaultSecLogTombstone();
	private static final LogEntryHeaderInterface MIGRATION_SEC_LOG_TOMBSTONE = new MigrationSecLogTombstone();

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
	 * @param p_logEntryHeader
	 *            the log entry header
	 * @return the number of written bytes
	 */
	public static int convertAndPut(final byte[] p_input, final int p_inputOffset, final byte[] p_output, final int p_outputOffset, final int p_logEntrySize,
			final int p_bytesUntilEnd, final LogEntryHeaderInterface p_logEntryHeader) {
		int ret = 0;
		short conversionOffset;

		conversionOffset = p_logEntryHeader.getConversionOffset();
		if (p_bytesUntilEnd >= p_logEntrySize || p_bytesUntilEnd <= 0) {
			// Set type field
			p_output[p_outputOffset] = p_input[p_inputOffset];
			// Copy shortened header and payload
			System.arraycopy(p_input, p_inputOffset + conversionOffset, p_output, p_outputOffset + 1, p_logEntrySize - conversionOffset);
			ret = p_logEntrySize - conversionOffset + 1;
		} else {
			// Entry is bisected
			if (p_bytesUntilEnd > conversionOffset) {
				// Set type field
				p_output[p_outputOffset] = p_input[p_inputOffset];
				// Copy shortened header and payload in two steps
				System.arraycopy(p_input, p_inputOffset + conversionOffset, p_output, p_outputOffset + 1, p_bytesUntilEnd - conversionOffset);
				ret += p_bytesUntilEnd - conversionOffset + 1;
				System.arraycopy(p_input, 0, p_output, p_outputOffset + p_bytesUntilEnd - conversionOffset, p_logEntrySize - p_bytesUntilEnd);
				ret += p_logEntrySize - p_bytesUntilEnd;
			} else {
				// Set type field
				p_output[p_outputOffset + conversionOffset - p_bytesUntilEnd] = p_input[0];
				// Copy shortened header and payload
				System.arraycopy(p_input, 0, p_output, p_outputOffset + conversionOffset - p_bytesUntilEnd + 1, p_logEntrySize
						- (conversionOffset - p_bytesUntilEnd));
				ret = p_logEntrySize - (conversionOffset - p_bytesUntilEnd) + 1;
			}
		}

		return ret;
	}

	/**
	 * Marks the log entry as invalid
	 * @param p_buffer
	 *            the buffer
	 * @param p_offset
	 *            the offset in buffer
	 * @param p_logStoresMigrations
	 *            whether the entry is in a secondary log for migrations or not
	 */
	public static void markAsInvalid(final byte[] p_buffer, final int p_offset, final boolean p_logStoresMigrations) {
		final byte invalid = (byte) 0xFF;
		int offset = p_offset;

		if (p_logStoresMigrations) {
			offset += LogHandler.LOG_ENTRY_NID_SIZE;
		}

		// LocalID
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
	public static void putType(final byte[] p_logEntry, final byte p_type, final short p_offset) {
		p_logEntry[p_offset] = p_type;
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
	public static void putRangeID(final byte[] p_logEntry, final byte p_rangeID, final short p_offset) {
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
	public static void putSource(final byte[] p_logEntry, final short p_source, final short p_offset) {
		for (int i = 0; i < LogHandler.LOG_ENTRY_SRC_SIZE; i++) {
			p_logEntry[p_offset + i] = (byte) (p_source >> i * 8 & 0xff);
		}
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
	public static void putChunkID(final byte[] p_logEntry, final long p_chunkID, final short p_offset) {
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
	public static void putLength(final byte[] p_logEntry, final byte p_length, final short p_offset) {
		p_logEntry[p_offset] = (byte) (p_length & 0xff);
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
	public static void putLength(final byte[] p_logEntry, final short p_length, final short p_offset) {
		for (int i = 0; i < Short.BYTES; i++) {
			p_logEntry[p_offset + i] = (byte) (p_length >> i * 8 & 0xff);
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
	public static void putLength(final byte[] p_logEntry, final int p_length, final short p_offset) {
		for (int i = 0; i < LogHandler.DEF_LOG_ENTRY_LEN_SIZE; i++) {
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
	public static void putVersion(final byte[] p_logEntry, final byte p_version, final short p_offset) {
		p_logEntry[p_offset] = (byte) (p_version & 0xff);
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
	public static void putVersion(final byte[] p_logEntry, final short p_version, final short p_offset) {
		for (int i = 0; i < Short.BYTES; i++) {
			p_logEntry[p_offset + i] = (byte) (p_version >> i * 8 & 0xff);
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
	public static void putVersion(final byte[] p_logEntry, final int p_version, final short p_offset) {
		for (int i = 0; i < LogHandler.DEF_LOG_ENTRY_VER_SIZE; i++) {
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
	public static void putChecksum(final byte[] p_logEntry, final long p_checksum, final short p_offset) {
		for (int i = 0; i < LogHandler.LOG_ENTRY_CRC_SIZE; i++) {
			p_logEntry[p_offset + i] = (byte) (p_checksum >> i * 8 & 0xff);
		}
	}

	/**
	 * Calculates the CRC32 checksum of a log entry's payload
	 * @param p_payload
	 *            the payload
	 * @return the checksum
	 */
	public static long calculateChecksumOfPayload(final byte[] p_payload) {

		CRC.reset();
		CRC.update(p_payload, 0, p_payload.length);

		return CRC.getValue();
	}

	/**
	 * Returns the maximum log entry header size for secondary log
	 * @param p_logStoresMigrations
	 *            whether the entry is in a secondary log for migrations or not
	 * @return the maximum log entry header size for secondary log
	 */
	public static int getMaxSecLogHeaderSize(final boolean p_logStoresMigrations) {
		int ret = -1;

		if (p_logStoresMigrations) {
			ret = MIGRATION_SEC_LOG_ENTRY_HEADER.getMaxHeaderSize();
		} else {
			ret = DEFAULT_SEC_LOG_ENTRY_HEADER.getMaxHeaderSize();
		}

		return ret;
	}

	/**
	 * Returns the corresponding LogEntryHeaderInterface of a primary log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the LogEntryHeaderInterface
	 */
	public static LogEntryHeaderInterface getPrimaryHeader(final byte[] p_buffer, final int p_offset) {
		LogEntryHeaderInterface ret = null;

		if (p_buffer[p_offset] == 0) {
			ret = DEFAULT_PRIM_LOG_ENTRY_HEADER;
		} else if (p_buffer[p_offset] == 1) {
			ret = MIGRATION_PRIM_LOG_ENTRY_HEADER;
		} else if (p_buffer[p_offset] == 2) {
			ret = DEFAULT_PRIM_LOG_TOMBSTONE;
		} else if (p_buffer[p_offset] == 3) {
			ret = MIGRATION_PRIM_LOG_TOMBSTONE;
		}

		return ret;
	}

	/**
	 * Returns the corresponding LogEntryHeaderInterface of a secondary log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @param p_logStoresMigrations
	 *            whether the entry is in a secondary log for migrations or not
	 * @return the LogEntryHeaderInterface
	 */
	public static LogEntryHeaderInterface getSecondaryHeader(final byte[] p_buffer, final int p_offset, final boolean p_logStoresMigrations) {
		LogEntryHeaderInterface ret = null;

		if (p_logStoresMigrations) {
			if (MIGRATION_SEC_LOG_TOMBSTONE.getVersion(p_buffer, p_offset) < 0) {
				ret = MIGRATION_SEC_LOG_TOMBSTONE;
			} else {
				ret = MIGRATION_SEC_LOG_ENTRY_HEADER;
			}

		} else {
			if (DEFAULT_SEC_LOG_TOMBSTONE.getVersion(p_buffer, p_offset) < 0) {
				ret = DEFAULT_SEC_LOG_TOMBSTONE;
			} else {
				ret = DEFAULT_SEC_LOG_ENTRY_HEADER;
			}
		}

		return ret;
	}
}
