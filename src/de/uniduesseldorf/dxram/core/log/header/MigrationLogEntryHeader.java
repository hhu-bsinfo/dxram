
package de.uniduesseldorf.dxram.core.log.header;

import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.log.LogHandler;

/**
 * Implements a log entry header for migration
 * @author Kevin Beineke
 *         25.06.2015
 */
public class MigrationLogEntryHeader implements LogEntryHeaderInterface {
	// Attributes
	public static final short SIZE = 28;
	public static final byte RID_OFFSET = LogHandler.LOG_ENTRY_TYP_SIZE;
	public static final byte SRC_OFFSET = RID_OFFSET + LogHandler.LOG_ENTRY_RID_SIZE;
	public static final byte NID_OFFSET = SRC_OFFSET + LogHandler.LOG_ENTRY_SRC_SIZE;
	public static final byte LID_OFFSET = NID_OFFSET + LogHandler.LOG_ENTRY_NID_SIZE;
	public static final byte LEN_OFFSET = LID_OFFSET + LogHandler.LOG_ENTRY_LID_SIZE;
	public static final byte VER_OFFSET = LEN_OFFSET + LogHandler.LOG_ENTRY_LEN_SIZE;
	public static final byte CRC_OFFSET = VER_OFFSET + LogHandler.LOG_ENTRY_VER_SIZE;

	// Constructors
	/**
	 * Creates an instance of MigrationLogEntryHeader
	 */
	public MigrationLogEntryHeader() {}

	// Methods
	@Override
	public byte[] createHeader(final Chunk p_chunk, final byte p_rangeID, final short p_source) {
		byte[] result;

		result = new byte[SIZE];
		AbstractLogEntryHeader.putType(result, (byte) 1, (byte) 0);
		AbstractLogEntryHeader.putRangeID(result, p_rangeID, RID_OFFSET);
		AbstractLogEntryHeader.putSource(result, p_source, SRC_OFFSET);
		AbstractLogEntryHeader.putChunkID(result, p_chunk.getChunkID(), NID_OFFSET);
		AbstractLogEntryHeader.putLength(result, p_chunk.getSize(), LEN_OFFSET);
		AbstractLogEntryHeader.putVersion(result, p_chunk.getVersion(), VER_OFFSET);
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
	@Override
	public byte getRangeID(final byte[] p_buffer, final int p_offset) {
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
	@Override
	public short getSource(final byte[] p_buffer, final int p_offset) {
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
	@Override
	public short getNodeID(final byte[] p_buffer, final int p_offset) {
		final int offset = p_offset + NID_OFFSET;

		return (short) ((p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8));
	}

	@Override
	public long getLID(final byte[] p_buffer, final int p_offset) {
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
	@Override
	public long getChunkID(final byte[] p_buffer, final int p_offset) {
		return ((long) getNodeID(p_buffer, p_offset) << 48) + getLID(p_buffer, p_offset);
	}

	@Override
	public int getLength(final byte[] p_buffer, final int p_offset) {
		final int offset = p_offset + LEN_OFFSET;

		return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
				+ ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24);
	}

	@Override
	public int getVersion(final byte[] p_buffer, final int p_offset) {
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
	@Override
	public long getChecksum(final byte[] p_buffer, final int p_offset) {
		final int offset = p_offset + CRC_OFFSET;

		return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
				+ ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24)
				+ ((p_buffer[offset + 4] & 0xff) << 32) + ((p_buffer[offset + 5] & 0xff) << 40)
				+ ((p_buffer[offset + 6] & 0xff) << 48) + ((p_buffer[offset + 7] & 0xff) << 54);
	}

	@Override
	public void print(final byte[] p_buffer, final int p_offset) {
		System.out.println("********************Primary Log Entry Header (Migration)********************");
		System.out.println("* NodeID: " + getNodeID(p_buffer, p_offset));
		System.out.println("* LocalID: " + getLID(p_buffer, p_offset));
		System.out.println("* Length: " + getLength(p_buffer, p_offset));
		System.out.println("* Version: " + getVersion(p_buffer, p_offset));
		System.out.println("* Checksum: " + getChecksum(p_buffer, p_offset));
		System.out.println("****************************************************************************");
	}

	@Override
	public short getHeaderSize() {
		return SIZE;
	}

	@Override
	public short getRIDOffset() {
		return RID_OFFSET;
	}

	@Override
	public short getSRCOffset() {
		return SRC_OFFSET;
	}

	@Override
	public short getNIDOffset() {
		return NID_OFFSET;
	}

	@Override
	public short getLIDOffset() {
		return LID_OFFSET;
	}

	@Override
	public short getLENOffset() {
		return LEN_OFFSET;
	}

	@Override
	public short getVEROffset() {
		return VER_OFFSET;
	}
}
