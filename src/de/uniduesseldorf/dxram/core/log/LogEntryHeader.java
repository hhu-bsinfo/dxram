
package de.uniduesseldorf.dxram.core.log;

import java.util.zip.CRC32;
import java.util.zip.Checksum;

import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.chunk.Chunk;

public class LogEntryHeader {

	private static Checksum m_crc = new CRC32();

	/**
	 * Puts type of log entry in log entry header
	 * @param p_logEntry
	 *            log entry
	 * @param p_type
	 *            the type (0 => normal, 1 => migration)
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
	 */
	public static void putChunkID(final byte[] p_logEntry, final long p_chunkID, final byte p_offset) {
		// NodeID
		for (int i = 0; i < LogHandler.LOG_ENTRY_NID_SIZE; i++) {
			p_logEntry[p_offset + i] = (byte) ((ChunkID.getCreatorID(p_chunkID) >> (i * 8)) & 0xff);
		}

		// LocalID
		for (int i = 0; i < LogHandler.LOG_ENTRY_LID_SIZE; i++) {
			p_logEntry[p_offset + LogHandler.LOG_ENTRY_NID_SIZE + i] = (byte) ((p_chunkID >> (i * 8)) & 0xff);
		}
	}

	/**
	 * Puts length of log entry in log entry header
	 * @param p_logEntry
	 *            log entry
	 * @param p_length
	 *            the length
	 */
	public static void putLength(final byte[] p_logEntry, final int p_length, final byte p_offset) {
		for (int i = 0; i < LogHandler.LOG_ENTRY_LEN_SIZE; i++) {
			p_logEntry[p_offset + i] = (byte) ((p_length >> (i * 8)) & 0xff);
		}
	}

	/**
	 * Puts version of log entry in log entry header
	 * @param p_logEntry
	 *            log entry
	 * @param p_version
	 *            the version
	 */
	public static void putVersion(final byte[] p_logEntry, final int p_version, final byte p_offset) {
		for (int i = 0; i < LogHandler.LOG_ENTRY_VER_SIZE; i++) {
			p_logEntry[p_offset + i] = (byte) ((p_version >> (i * 8)) & 0xff);
		}
	}

	/**
	 * Puts length of log entry in log entry header
	 * @param p_logEntry
	 *            log entry
	 * @param p_checksum
	 *            the checksum
	 */
	public static void putChecksum(final byte[] p_logEntry, final long p_checksum, final byte p_offset) {
		for (int i = 0; i < LogHandler.LOG_ENTRY_CRC_SIZE; i++) {
			p_logEntry[p_offset + i] = (byte) ((p_checksum >> (i * 8)) & 0xff);
		}
	}

	/**
	 * Puts RangeID of log entry in log entry header
	 * @param p_logEntry
	 *            log entry
	 * @param p_rangeID
	 *            the RangeID
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
	 */
	public static void putSource(final byte[] p_logEntry, final short p_source, final byte p_offset) {
		for (int i = 0; i < LogHandler.LOG_ENTRY_SRC_SIZE; i++) {
			p_logEntry[p_offset + i] = (byte) ((p_source >> (i * 8)) & 0xff);
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
	public static byte getType(final byte[] p_buffer, final int p_offset) {
		return p_buffer[0];
	}

	public static class Primary {
		public static final int SIZE = 25;
		public static final byte NID_OFFSET = LogHandler.LOG_ENTRY_TYP_SIZE;
		public static final byte LID_OFFSET = NID_OFFSET + LogHandler.LOG_ENTRY_NID_SIZE;
		public static final byte LEN_OFFSET = LID_OFFSET + LogHandler.LOG_ENTRY_LID_SIZE;
		public static final byte VER_OFFSET = LEN_OFFSET + LogHandler.LOG_ENTRY_LEN_SIZE;
		public static final byte CRC_OFFSET = VER_OFFSET + LogHandler.LOG_ENTRY_VER_SIZE;

		/**
		 * Generates a log entry with filled-in header but without any payload
		 * @param p_chunk
		 *            the Chunk
		 * @return the log entry
		 */
		public static byte[] createHeader(final Chunk p_chunk) {
			byte[] result;

			result = new byte[SIZE];
			putType(result, (byte) 0, (byte) 0);
			putChunkID(result, p_chunk.getChunkID(), NID_OFFSET);
			putLength(result, p_chunk.getSize(), LEN_OFFSET);
			putVersion(result, p_chunk.getVersion(), VER_OFFSET);
			// putChecksumInLogEntryHeader(result, calculateChecksumOfPayload(p_chunk.getData().array()), CRC_OFFSET);

			return result;
		}

		/**
		 * Returns NodeID of a log entry
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the NodeID
		 */
		public static short getNodeID(final byte[] p_buffer, final int p_offset) {
			final int offset = p_offset + NID_OFFSET;

			return (short) ((p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8));
		}

		/**
		 * Returns the LID of a log entry
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the LID
		 */
		public static long getLID(final byte[] p_buffer, final int p_offset) {
			final int offset = p_offset + LID_OFFSET;

			return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
					+ ((p_buffer[offset + 2] & 0xff) << 16) + (((long) p_buffer[offset + 3] & 0xff) << 24)
					+ (((long) p_buffer[offset + 4] & 0xff) << 32) + (((long) p_buffer[offset + 5] & 0xff) << 40);
		}

		/**
		 * Returns the ChunkID of a log entry
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the ChunkID
		 */
		public static long getChunkID(final byte[] p_buffer, final int p_offset) {
			return ((long) getNodeID(p_buffer, p_offset) << 48) + getLID(p_buffer, p_offset);
		}

		/**
		 * Returns length of a log entry
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the length
		 */
		public static int getLength(final byte[] p_buffer, final int p_offset) {
			final int offset = p_offset + LEN_OFFSET;

			return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
					+ ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24);
		}

		/**
		 * Returns version of a log entry
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the version
		 */
		public static int getVersion(final byte[] p_buffer, final int p_offset) {
			final int offset = p_offset + VER_OFFSET;

			return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
					+ ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24);
		}

		/**
		 * Returns the checksum of a log entry's payload
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the checksum
		 */
		public static long getChecksum(final byte[] p_buffer, final int p_offset) {
			final int offset = p_offset + CRC_OFFSET;

			return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
					+ ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24)
					+ ((p_buffer[offset + 4] & 0xff) << 32) + ((p_buffer[offset + 5] & 0xff) << 40)
					+ ((p_buffer[offset + 6] & 0xff) << 48) + ((p_buffer[offset + 7] & 0xff) << 54);
		}

		/**
		 * Prints the log header
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 */
		public static void print(final byte[] p_buffer, final int p_offset) {
			System.out.println("********************Primary Log Entry Header********************");
			System.out.println("* NodeID: " + getNodeID(p_buffer, p_offset));
			System.out.println("* LocalID: " + getLID(p_buffer, p_offset));
			System.out.println("* Length: " + getLength(p_buffer, p_offset));
			System.out.println("* Version: " + getVersion(p_buffer, p_offset));
			System.out.println("* Checksum: " + getChecksum(p_buffer, p_offset));
			System.out.println("****************************************************************");
		}
	}

	public static class Migration {
		public static final int SIZE = 28;
		public static final byte RID_OFFSET = LogHandler.LOG_ENTRY_TYP_SIZE;
		public static final byte SRC_OFFSET = RID_OFFSET + LogHandler.LOG_ENTRY_RID_SIZE;
		public static final byte NID_OFFSET = SRC_OFFSET + LogHandler.LOG_ENTRY_SRC_SIZE;
		public static final byte LID_OFFSET = NID_OFFSET + LogHandler.LOG_ENTRY_NID_SIZE;
		public static final byte LEN_OFFSET = LID_OFFSET + LogHandler.LOG_ENTRY_LID_SIZE;
		public static final byte VER_OFFSET = LEN_OFFSET + LogHandler.LOG_ENTRY_LEN_SIZE;
		public static final byte CRC_OFFSET = VER_OFFSET + LogHandler.LOG_ENTRY_VER_SIZE;

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
		public static byte[] createHeader(final Chunk p_chunk, final byte p_rangeID, final short p_source) {
			byte[] result;

			result = new byte[SIZE];
			putType(result, (byte) 1, (byte) 0);
			putRangeID(result, p_rangeID, RID_OFFSET);
			putSource(result, p_source, SRC_OFFSET);
			putChunkID(result, p_chunk.getChunkID(), NID_OFFSET);
			putLength(result, p_chunk.getSize(), LEN_OFFSET);
			putVersion(result, p_chunk.getVersion(), VER_OFFSET);
			// putChecksumInLogEntryHeader(result, calculateChecksumOfPayload(p_chunk.getData().array()), CRC_OFFSET);

			return result;
		}

		/**
		 * Returns RangeID of a log entry
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the version
		 */
		public static byte getRangeID(final byte[] p_buffer, final int p_offset) {
			return p_buffer[RID_OFFSET];
		}

		/**
		 * Returns source of a log entry
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the NodeID
		 */
		public static short getSource(final byte[] p_buffer, final int p_offset) {
			final int offset = p_offset + SRC_OFFSET;

			return (short) ((p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8));
		}

		/**
		 * Returns NodeID of a log entry
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the NodeID
		 */
		public static short getNodeID(final byte[] p_buffer, final int p_offset) {
			final int offset = p_offset + NID_OFFSET;

			return (short) ((p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8));
		}

		/**
		 * Returns the LID of a log entry
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the LID
		 */
		public static long getLID(final byte[] p_buffer, final int p_offset) {
			final int offset = p_offset + LID_OFFSET;

			return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
					+ ((p_buffer[offset + 2] & 0xff) << 16) + (((long) p_buffer[offset + 3] & 0xff) << 24)
					+ (((long) p_buffer[offset + 4] & 0xff) << 32) + (((long) p_buffer[offset + 5] & 0xff) << 40);
		}

		/**
		 * Returns the ChunkID of a log entry
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the ChunkID
		 */
		public static long getChunkID(final byte[] p_buffer, final int p_offset) {
			return ((long) getNodeID(p_buffer, p_offset) << 48) + getLID(p_buffer, p_offset);
		}

		/**
		 * Returns length of a log entry
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the length
		 */
		public static int getLength(final byte[] p_buffer, final int p_offset) {
			final int offset = p_offset + LEN_OFFSET;

			return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
					+ ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24);
		}

		/**
		 * Returns version of a log entry
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the version
		 */
		public static int getVersion(final byte[] p_buffer, final int p_offset) {
			final int offset = p_offset + VER_OFFSET;

			return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
					+ ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24);
		}

		/**
		 * Returns the checksum of a log entry's payload
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the checksum
		 */
		public static long getChecksum(final byte[] p_buffer, final int p_offset) {
			final int offset = p_offset + CRC_OFFSET;

			return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
					+ ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24)
					+ ((p_buffer[offset + 4] & 0xff) << 32) + ((p_buffer[offset + 5] & 0xff) << 40)
					+ ((p_buffer[offset + 6] & 0xff) << 48) + ((p_buffer[offset + 7] & 0xff) << 54);
		}

		/**
		 * Prints the log header
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 */
		public static void print(final byte[] p_buffer, final int p_offset) {
			System.out.println("********************Primary Log Entry Header********************");
			System.out.println("* NodeID: " + getNodeID(p_buffer, p_offset));
			System.out.println("* LocalID: " + getLID(p_buffer, p_offset));
			System.out.println("* Length: " + getLength(p_buffer, p_offset));
			System.out.println("* Version: " + getVersion(p_buffer, p_offset));
			System.out.println("* Checksum: " + getChecksum(p_buffer, p_offset));
			System.out.println("****************************************************************");
		}
	}

	public static class Tombstone {
		public static final int SIZE = 17;
		public static final byte NID_OFFSET = LogHandler.LOG_ENTRY_TYP_SIZE;
		public static final byte LID_OFFSET = NID_OFFSET + LogHandler.LOG_ENTRY_NID_SIZE;
		public static final byte LEN_OFFSET = LID_OFFSET + LogHandler.LOG_ENTRY_LID_SIZE;
		public static final byte VER_OFFSET = LEN_OFFSET + LogHandler.LOG_ENTRY_LEN_SIZE;

		/**
		 * Generates a log entry with filled-in header but without any payload
		 * @param p_chunkID
		 *            the ChunkID
		 * @return the log entry
		 */
		public static byte[] createHeader(final long p_chunkID) {
			byte[] result;

			result = new byte[SIZE];
			putType(result, (byte) 2, (byte) 0);
			putChunkID(result, p_chunkID, NID_OFFSET);
			putLength(result, 0, LEN_OFFSET);
			putVersion(result, -1, VER_OFFSET);

			return result;
		}

		/**
		 * Returns NodeID of a log entry
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the NodeID
		 */
		public static short getNodeID(final byte[] p_buffer, final int p_offset) {
			final int offset = p_offset + NID_OFFSET;

			return (short) ((p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8));
		}

		/**
		 * Returns the LID of a log entry
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the LID
		 */
		public static long getLID(final byte[] p_buffer, final int p_offset) {
			final int offset = p_offset + LID_OFFSET;

			return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
					+ ((p_buffer[offset + 2] & 0xff) << 16) + (((long) p_buffer[offset + 3] & 0xff) << 24)
					+ (((long) p_buffer[offset + 4] & 0xff) << 32) + (((long) p_buffer[offset + 5] & 0xff) << 40);
		}

		/**
		 * Returns the ChunkID of a log entry
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the ChunkID
		 */
		public static long getChunkID(final byte[] p_buffer, final int p_offset) {
			return ((long) getNodeID(p_buffer, p_offset) << 48) + getLID(p_buffer, p_offset);
		}

		/**
		 * Returns length of a log entry
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the length
		 */
		public static int getLength(final byte[] p_buffer, final int p_offset) {
			final int offset = p_offset + LEN_OFFSET;

			return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
					+ ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24);
		}

		/**
		 * Returns version of a log entry
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the version
		 */
		public static int getVersion(final byte[] p_buffer, final int p_offset) {
			final int offset = p_offset + VER_OFFSET;

			return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
					+ ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24);
		}

		/**
		 * Prints the log header
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 */
		public static void print(final byte[] p_buffer, final int p_offset) {
			System.out.println("********************Tombstone********************");
			System.out.println("* NodeID: " + getNodeID(p_buffer, p_offset));
			System.out.println("* LocalID: " + getLID(p_buffer, p_offset));
			System.out.println("* Length: " + getLength(p_buffer, p_offset));
			System.out.println("* Version: " + getVersion(p_buffer, p_offset));
			System.out.println("*************************************************");
		}
	}

	public static class Secondary {
		public static final int SIZE = 22;
		public static final byte LEN_OFFSET = LogHandler.LOG_ENTRY_LID_SIZE;
		public static final byte VER_OFFSET = LEN_OFFSET + LogHandler.LOG_ENTRY_LEN_SIZE;
		public static final byte CRC_OFFSET = VER_OFFSET + LogHandler.LOG_ENTRY_VER_SIZE;

		public static void convertHeader(final byte[] p_input, final int p_inputOffset, final byte[] p_output,
				final byte[] p_outputOffset) {
			switch (getType(p_input, p_inputOffset)) {
			case 0:
				convertHeaderPrimary(p_input, p_inputOffset, p_output, p_outputOffset);
				break;
			case 1:
				convertHeaderMigration(p_input, p_inputOffset, p_output, p_outputOffset);
				break;
			case 2:
				convertHeaderTombstone(p_input, p_inputOffset, p_output, p_outputOffset);
				break;
			default:
				break;
			}

		}

		public static void convertHeaderPrimary(final byte[] p_input, final int p_inputOffset, final byte[] p_output,
				final byte[] p_outputOffset) {

		}

		public static void convertHeaderMigration(final byte[] p_input, final int p_inputOffset, final byte[] p_output,
				final byte[] p_outputOffset) {

		}

		public static void convertHeaderTombstone(final byte[] p_input, final int p_inputOffset, final byte[] p_output,
				final byte[] p_outputOffset) {

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
		 * Returns the LID of a log entry
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the LID
		 */
		public static long getLID(final byte[] p_buffer, final int p_offset) {
			return (p_buffer[p_offset] & 0xff) + ((p_buffer[p_offset + 1] & 0xff) << 8)
					+ ((p_buffer[p_offset + 2] & 0xff) << 16) + (((long) p_buffer[p_offset + 3] & 0xff) << 24)
					+ (((long) p_buffer[p_offset + 4] & 0xff) << 32) + (((long) p_buffer[p_offset + 5] & 0xff) << 40);
		}

		/**
		 * Returns length of a log entry
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the length
		 */
		public static int getLength(final byte[] p_buffer, final int p_offset) {
			final int offset = p_offset + LEN_OFFSET;

			return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
					+ ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24);
		}

		/**
		 * Returns version of a log entry
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the version
		 */
		public static int getVersion(final byte[] p_buffer, final int p_offset) {
			final int offset = p_offset + VER_OFFSET;

			return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
					+ ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24);
		}

		/**
		 * Returns the checksum of a log entry's payload
		 * @param p_buffer
		 *            buffer with log entries
		 * @param p_offset
		 *            offset in buffer
		 * @return the checksum
		 */
		public static long getChecksum(final byte[] p_buffer, final int p_offset) {
			final int offset = p_offset + CRC_OFFSET;

			return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
					+ ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24)
					+ ((p_buffer[offset + 4] & 0xff) << 32) + ((p_buffer[offset + 5] & 0xff) << 40)
					+ ((p_buffer[offset + 6] & 0xff) << 48) + ((p_buffer[offset + 7] & 0xff) << 54);
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
		public static void print(final byte[] p_buffer, final int p_offset, final boolean p_primary) {
			System.out.println("********************Secondary Log Entry Header********************");
			System.out.println("* LocalID: " + getLID(p_buffer, p_offset));
			System.out.println("* Length: " + getLength(p_buffer, p_offset));
			System.out.println("* Version: " + getVersion(p_buffer, p_offset));
			System.out.println("* Checksum: " + getChecksum(p_buffer, p_offset));
			System.out.println("******************************************************************");
		}
	}
}
