
package de.uniduesseldorf.dxram.core.log.header;

import java.util.zip.CRC32;
import java.util.zip.Checksum;

import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.chunk.Chunk;

/**
 * A helper class for the LogEntryHeaderInterface.
 * Provides methods to detect the type of a log entry header or to convert one.
 * @author Kevin Beineke
 *         25.06.2015
 */
public abstract class AbstractLogEntryHeader {

	// Constants
	private static final byte PRIMLOG_TYPE_MASK = (byte) 0x03;
	private static final byte SECLOG_TYPE_MASK = (byte) 0x01;

	protected static final byte INVALIDATION_MASK = (byte) 0x02;
	protected static final byte LID_LENGTH_MASK = (byte) 0x0C;
	protected static final byte LEN_LENGTH_MASK = (byte) 0x30;
	protected static final byte VER_LENGTH_MASK = (byte) 0xC0;
	protected static final byte LID_LENGTH_SHFT = (byte) 2;
	protected static final byte LEN_LENGTH_SHFT = (byte) 4;
	protected static final byte VER_LENGTH_SHFT = (byte) 6;

	protected static final byte LOG_ENTRY_TYP_SIZE = 1;
	protected static final byte LOG_ENTRY_NID_SIZE = 2;
	protected static final byte MAX_LOG_ENTRY_LID_SIZE = 6;
	protected static final byte MAX_LOG_ENTRY_CID_SIZE = LOG_ENTRY_NID_SIZE + MAX_LOG_ENTRY_LID_SIZE;
	protected static final byte MAX_LOG_ENTRY_LEN_SIZE = 3;
	protected static final byte MAX_LOG_ENTRY_VER_SIZE = 3;
	protected static final byte LOG_ENTRY_RID_SIZE = 1;
	protected static final byte LOG_ENTRY_SRC_SIZE = 2;
	protected static final byte LOG_ENTRY_CRC_SIZE;
	protected static final boolean USE_CHECKSUM = Core.getConfiguration().getBooleanValue(ConfigurationConstants.LOG_CHECKSUM);
	static {
		if (USE_CHECKSUM) {
			LOG_ENTRY_CRC_SIZE = (byte) 4;
		} else {
			LOG_ENTRY_CRC_SIZE = (byte) 0;
		}
	}

	// Attributes
	private static final Checksum CRC = new CRC32();
	private static final AbstractLogEntryHeader DEFAULT_PRIM_LOG_ENTRY_HEADER = new DefaultPrimLogEntryHeader();
	private static final AbstractLogEntryHeader MIGRATION_PRIM_LOG_ENTRY_HEADER = new MigrationPrimLogEntryHeader();
	private static final AbstractLogEntryHeader DEFAULT_PRIM_LOG_TOMBSTONE = new DefaultPrimLogTombstone();
	private static final AbstractLogEntryHeader MIGRATION_PRIM_LOG_TOMBSTONE = new MigrationPrimLogTombstone();

	private static final AbstractLogEntryHeader DEFAULT_SEC_LOG_ENTRY_HEADER = new DefaultSecLogEntryHeader();
	private static final AbstractLogEntryHeader MIGRATION_SEC_LOG_ENTRY_HEADER = new MigrationSecLogEntryHeader();
	private static final AbstractLogEntryHeader DEFAULT_SEC_LOG_TOMBSTONE = new DefaultSecLogTombstone();
	private static final AbstractLogEntryHeader MIGRATION_SEC_LOG_TOMBSTONE = new MigrationSecLogTombstone();

	// Constructors
	/**
	 * Creates an instance of LogEntryHeaderHelper
	 */
	protected AbstractLogEntryHeader() {}

	// Methods
	/**
	 * Generates a log entry with filled-in header but without any payload
	 * @param p_chunk
	 *            the Chunk
	 * @param p_rangeID
	 *            the RangeID
	 * @param p_source
	 *            the source NodeID
	 * @return the log entry
	 */
	public abstract byte[] createLogEntryHeader(final Chunk p_chunk, final byte p_rangeID, final short p_source);

	/**
	 * Generates a tombstone
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_version
	 *            the version
	 * @param p_rangeID
	 *            the RangeID
	 * @param p_source
	 *            the source NodeID
	 * @return the log entry
	 */
	public abstract byte[] createTombstone(final long p_chunkID, final int p_version, final byte p_rangeID, final short p_source);

	/**
	 * Returns the type of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the type
	 */
	protected abstract short getType(final byte[] p_buffer, final int p_offset);

	/**
	 * Returns RangeID of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the version
	 */
	public abstract byte getRangeID(final byte[] p_buffer, final int p_offset);

	/**
	 * Returns source of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the NodeID
	 */
	public abstract short getSource(final byte[] p_buffer, final int p_offset);

	/**
	 * Returns NodeID of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the NodeID
	 */
	public abstract short getNodeID(final byte[] p_buffer, final int p_offset);

	/**
	 * Returns the LocalID of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the LocalID
	 */
	public abstract long getLID(final byte[] p_buffer, final int p_offset);

	/**
	 * Returns the ChunkID of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the ChunkID
	 */
	public abstract long getChunkID(final byte[] p_buffer, final int p_offset);

	/**
	 * Returns length of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the length
	 */
	public abstract int getLength(final byte[] p_buffer, final int p_offset);

	/**
	 * Returns version of a log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the version
	 */
	public abstract int getVersion(final byte[] p_buffer, final int p_offset);

	/**
	 * Returns the checksum of a log entry's payload
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the checksum
	 */
	public abstract int getChecksum(final byte[] p_buffer, final int p_offset);

	/**
	 * Checks whether this log entry was migrated or not
	 * @return whether this log entry was migrated or not
	 */
	public abstract boolean wasMigrated();

	/**
	 * Checks whether this log entry is a tombstone or not
	 * @return whether this log entry is a tombstone or not
	 */
	public abstract boolean isTombstone();

	/**
	 * Checks whether this log entry is invalid or not
	 * @param p_buffer
	 *            the buffer
	 * @param p_offset
	 *            the offset in buffer
	 * @return whether this log entry is invalid or not
	 */
	public abstract boolean isInvalid(final byte[] p_buffer, final int p_offset);

	/**
	 * Returns the log entry header size
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the size
	 */
	public abstract short getHeaderSize(final byte[] p_buffer, final int p_offset);

	/**
	 * Returns the maximum log entry header size
	 * @return the size
	 */
	public abstract short getMaxHeaderSize();

	/**
	 * Returns the offset for conversion
	 * @return the offset
	 */
	public abstract short getConversionOffset();

	/**
	 * Returns whether the length field is completely in this iteration or not
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @param p_bytesUntilEnd
	 *            number of bytes until wrap around
	 * @return whether the length field is completely in this iteration or not
	 */
	public abstract boolean readable(final byte[] p_buffer, final int p_offset, final int p_bytesUntilEnd);

	/**
	 * Returns the NodeID offset
	 * @return the offset
	 */
	protected abstract short getNIDOffset();

	/**
	 * Returns the LocalID offset
	 * @return the offset
	 */
	protected abstract short getLIDOffset();

	/**
	 * Returns the length offset
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the offset
	 */
	protected abstract short getLENOffset(final byte[] p_buffer, final int p_offset);

	/**
	 * Returns the version offset
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the offset
	 */
	protected abstract short getVEROffset(final byte[] p_buffer, final int p_offset);

	/**
	 * Returns the checksum offset
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the offset
	 */
	protected abstract short getCRCOffset(final byte[] p_buffer, final int p_offset);

	/**
	 * Prints the log header
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 */
	public abstract void print(final byte[] p_buffer, final int p_offset);

	/**
	 * Returns the corresponding AbstractLogEntryHeader of a primary log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the AbstractLogEntryHeader
	 */
	public static AbstractLogEntryHeader getPrimaryHeader(final byte[] p_buffer, final int p_offset) {
		AbstractLogEntryHeader ret = null;
		byte type;

		type = (byte) (p_buffer[p_offset] & PRIMLOG_TYPE_MASK);
		if (type == 0) {
			ret = DEFAULT_PRIM_LOG_ENTRY_HEADER;
		} else if (type == 1) {
			ret = DEFAULT_PRIM_LOG_TOMBSTONE;
		} else if (type == 2) {
			ret = MIGRATION_PRIM_LOG_ENTRY_HEADER;
		} else if (type == 3) {
			ret = MIGRATION_PRIM_LOG_TOMBSTONE;
		} else {
			System.out.println("Error: Type of log entry header unknown!");
		}

		return ret;
	}

	/**
	 * Returns the corresponding AbstractLogEntryHeader of a secondary log entry
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @param p_storesMigrations
	 *            whether the secondary log this entry is in stores migrations or not
	 * @return the AbstractLogEntryHeader
	 */
	public static AbstractLogEntryHeader getSecondaryHeader(final byte[] p_buffer, final int p_offset, final boolean p_storesMigrations) {
		AbstractLogEntryHeader ret = null;
		byte type;

		type = (byte) (p_buffer[p_offset] & SECLOG_TYPE_MASK);
		if (type == 0) {
			if (!p_storesMigrations) {
				ret = DEFAULT_SEC_LOG_ENTRY_HEADER;
			} else {
				ret = MIGRATION_SEC_LOG_ENTRY_HEADER;
			}
		} else if (type == 1) {
			if (!p_storesMigrations) {
				ret = DEFAULT_SEC_LOG_TOMBSTONE;
			} else {
				ret = MIGRATION_SEC_LOG_TOMBSTONE;
			}
		}

		return ret;
	}

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
			final int p_bytesUntilEnd, final AbstractLogEntryHeader p_logEntryHeader) {
		int ret = 0;
		short conversionOffset;

		conversionOffset = p_logEntryHeader.getConversionOffset();
		if (p_bytesUntilEnd >= p_logEntrySize || p_bytesUntilEnd <= 0) {
			// Set type field (only differentiate between log entry and tombstone, not migrations)
			p_output[p_outputOffset] = (byte) (p_input[p_inputOffset] & 0xFD);
			// Copy shortened header and payload
			System.arraycopy(p_input, p_inputOffset + conversionOffset, p_output, p_outputOffset + 1, p_logEntrySize - conversionOffset);
			ret = p_logEntrySize - conversionOffset + 1;
		} else {
			// Entry is bisected
			if (p_bytesUntilEnd > conversionOffset) {
				// Set type field (only differentiate between log entry and tombstone, not migrations)
				p_output[p_outputOffset] = (byte) (p_input[p_inputOffset] & 0xFD);
				// Copy shortened header and payload in two steps
				System.arraycopy(p_input, p_inputOffset + conversionOffset, p_output, p_outputOffset + 1, p_bytesUntilEnd - conversionOffset);
				ret += p_bytesUntilEnd - conversionOffset + 1;
				System.arraycopy(p_input, 0, p_output, p_outputOffset + p_bytesUntilEnd - conversionOffset, p_logEntrySize - p_bytesUntilEnd);
				ret += p_logEntrySize - p_bytesUntilEnd;
			} else {
				// Set type field (only differentiate between log entry and tombstone, not migrations)
				p_output[p_outputOffset + conversionOffset - p_bytesUntilEnd] = (byte) (p_input[0] & 0xFD);
				// Copy shortened header and payload
				System.arraycopy(p_input, 0, p_output, p_outputOffset + conversionOffset - p_bytesUntilEnd + 1, p_logEntrySize
						- (conversionOffset - p_bytesUntilEnd));
				ret = p_logEntrySize - (conversionOffset - p_bytesUntilEnd) + 1;
			}
		}

		return ret;
	}

	/**
	 * Calculates the CRC32 checksum of a log entry's payload
	 * @param p_payload
	 *            the payload
	 * @return the checksum
	 */
	public static int calculateChecksumOfPayload(final byte[] p_payload) {

		CRC.reset();
		CRC.update(p_payload, 0, p_payload.length);

		return (int) CRC.getValue();
	}

	/**
	 * Returns the maximum log entry header size for secondary log
	 * @param p_logStoresMigrations
	 *            whether the entry is in a secondary log for migrations or not
	 * @param p_localID
	 *            the LocalID
	 * @param p_size
	 *            the size
	 * @param p_version
	 *            the version
	 * @return the maximum log entry header size for secondary log
	 */
	public static short getSecLogHeaderSize(final boolean p_logStoresMigrations, final long p_localID, final int p_size, final int p_version) {
		short ret = (short) (LOG_ENTRY_TYP_SIZE + getSizeForLocalIDField(p_localID) + getSizeForLengthField(p_size)
				+ getSizeForVersionField(p_version) + LOG_ENTRY_CRC_SIZE);

		if (p_logStoresMigrations) {
			ret += LOG_ENTRY_NID_SIZE;
		}

		return ret;
	}

	/**
	 * Returns the maximum log entry header size for secondary log
	 * @param p_logStoresMigrations
	 *            whether the entry is in a secondary log for migrations or not
	 * @return the maximum log entry header size for secondary log
	 */
	public static short getMaxSecLogHeaderSize(final boolean p_logStoresMigrations) {
		short ret;

		if (p_logStoresMigrations) {
			ret = MIGRATION_SEC_LOG_ENTRY_HEADER.getMaxHeaderSize();
		} else {
			ret = DEFAULT_SEC_LOG_ENTRY_HEADER.getMaxHeaderSize();
		}

		return ret;
	}

	/**
	 * Marks the log entry as invalid
	 * @param p_buffer
	 *            the buffer
	 * @param p_offset
	 *            the offset in buffer
	 * @param p_logEntryHeader
	 *            the log entry header
	 */
	public static void markAsInvalid(final byte[] p_buffer, final int p_offset, final AbstractLogEntryHeader p_logEntryHeader) {
		p_buffer[p_offset] = (byte) (p_buffer[p_offset] | INVALIDATION_MASK);
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
	protected static void putType(final byte[] p_logEntry, final byte p_type, final short p_offset) {
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
	protected static void putRangeID(final byte[] p_logEntry, final byte p_rangeID, final short p_offset) {
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
	protected static void putSource(final byte[] p_logEntry, final short p_source, final short p_offset) {
		for (int i = 0; i < LOG_ENTRY_SRC_SIZE; i++) {
			p_logEntry[p_offset + i] = (byte) (p_source >> i * 8 & 0xff);
		}
	}

	/**
	 * Puts ChunkID in log entry header
	 * @param p_logEntry
	 *            log entry
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_localIDSize
	 *            the length of the LocalID field
	 * @param p_offset
	 *            the type-specific offset
	 */
	protected static void putChunkID(final byte[] p_logEntry, final long p_chunkID, final byte p_localIDSize, final short p_offset) {
		// NodeID
		for (int i = 0; i < LOG_ENTRY_NID_SIZE; i++) {
			p_logEntry[p_offset + i] = (byte) (ChunkID.getCreatorID(p_chunkID) >> i * 8 & 0xff);
		}

		// LocalID
		for (int i = 0; i < p_localIDSize; i++) {
			p_logEntry[p_offset + LOG_ENTRY_NID_SIZE + i] = (byte) (p_chunkID >> i * 8 & 0xff);
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
	protected static void putLength(final byte[] p_logEntry, final byte p_length, final short p_offset) {
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
	protected static void putLength(final byte[] p_logEntry, final short p_length, final short p_offset) {
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
	protected static void putLength(final byte[] p_logEntry, final int p_length, final short p_offset) {
		for (int i = 0; i < MAX_LOG_ENTRY_LEN_SIZE; i++) {
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
	protected static void putVersion(final byte[] p_logEntry, final byte p_version, final short p_offset) {
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
	protected static void putVersion(final byte[] p_logEntry, final short p_version, final short p_offset) {
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
	protected static void putVersion(final byte[] p_logEntry, final int p_version, final short p_offset) {
		for (int i = 0; i < MAX_LOG_ENTRY_VER_SIZE; i++) {
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
	protected static void putChecksum(final byte[] p_logEntry, final int p_checksum, final short p_offset) {
		for (int i = 0; i < LOG_ENTRY_CRC_SIZE; i++) {
			p_logEntry[p_offset + i] = (byte) (p_checksum >> i * 8 & 0xff);
		}
	}

	/**
	 * Returns the number of bytes necessary to store given value
	 * @param p_localID
	 *            the value
	 * @return the number of bytes necessary to store given value
	 */
	protected static byte getSizeForLocalIDField(final long p_localID) {
		byte ret;

		ret = (byte) Math.ceil(Math.log10(p_localID + 1) / Math.log10(2) / 8);

		// Only allow sizes 1, 2, 4 and 6, because there are only four states
		if (ret == 0) {
			ret = 1;
		} else if (ret == 3) {
			ret = 4;
		} else if (ret == 5) {
			ret = 6;
		}

		return ret;
	}

	/**
	 * Returns the number of bytes necessary to store given value
	 * @param p_length
	 *            the value
	 * @return the number of bytes necessary to store given value
	 */
	protected static byte getSizeForLengthField(final int p_length) {
		byte ret;

		ret = (byte) Math.ceil(Math.log10(p_length + 1) / Math.log10(2) / 8);

		if (ret > 3) {
			System.out.println("Error: Log Entry too long!");
		}

		return ret;
	}

	/**
	 * Returns the number of bytes necessary to store given value
	 * @param p_version
	 *            the value
	 * @return the number of bytes necessary to store given value
	 */
	protected static byte getSizeForVersionField(final int p_version) {
		byte ret = 0;

		if (p_version != 1) {
			ret = (byte) Math.ceil(Math.log10(p_version + 1) / Math.log10(2) / 8);

			if (ret > 3) {
				System.out.println("Error: Log Entry version too high!");
			}
		}

		return ret;
	}

	/**
	 * Returns the number of bytes necessary to store given value
	 * @param p_type
	 *            the type of the log entry header
	 * @param p_localIDSize
	 *            the size of the checksum field
	 * @param p_lengthSize
	 *            the size of the length field
	 * @param p_versionSize
	 *            the size of the version field
	 * @return the number of bytes necessary to store given value
	 */
	protected static byte generateTypeField(final byte p_type, final byte p_localIDSize, final byte p_lengthSize, final byte p_versionSize) {
		byte ret = p_type;

		// Length of LocalID is between 0 and 6 Bytes, but there are only 2 Bits for storing the length. Different
		// lengths: 1, 2, 4 ,6
		switch (p_localIDSize) {
		case 1:
			ret |= 0 << LID_LENGTH_SHFT;
			break;
		case 2:
			ret |= 1 << LID_LENGTH_SHFT;
			break;
		case 4:
			ret |= 2 << LID_LENGTH_SHFT;
			break;
		case 6:
			ret |= 3 << LID_LENGTH_SHFT;
			break;
		default:
			System.out.println("Error: Unknown LocalID!");
			break;
		}

		// Length of size is linear: 0, 1, 2, 3
		ret |= p_lengthSize << LEN_LENGTH_SHFT;

		// Length of version is linear, too: 0, 1, 2, 3
		ret |= p_versionSize << VER_LENGTH_SHFT;

		return ret;
	}
}
