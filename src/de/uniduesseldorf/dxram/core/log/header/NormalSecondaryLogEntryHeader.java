
package de.uniduesseldorf.dxram.core.log.header;

import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.log.LogHandler;

/**
 * Implements a log entry header for a normal secondary log
 * @author Kevin Beineke
 *         25.06.2015
 */
public class NormalSecondaryLogEntryHeader implements LogEntryHeaderInterface {
	// Attributes
	public static final short SIZE = 22;
	public static final byte LEN_OFFSET = LogHandler.LOG_ENTRY_LID_SIZE;
	public static final byte VER_OFFSET = LEN_OFFSET + LogHandler.LOG_ENTRY_LEN_SIZE;
	public static final byte CRC_OFFSET = VER_OFFSET + LogHandler.LOG_ENTRY_VER_SIZE;

	// Constructors
	/**
	 * Creates an instance of NormalSecondaryLogEntryHeader
	 */
	public NormalSecondaryLogEntryHeader() {}

	// Methods
	@Override
	public short getNodeID(final byte[] p_buffer, final int p_offset) {
		System.out.println("No NodeID in NormalSecondaryLogEntryHeader!");
		return -1;
	}

	@Override
	public long getLID(final byte[] p_buffer, final int p_offset) {
		return (p_buffer[p_offset] & 0xff) + ((p_buffer[p_offset + 1] & 0xff) << 8)
				+ ((p_buffer[p_offset + 2] & 0xff) << 16) + (((long) p_buffer[p_offset + 3] & 0xff) << 24)
				+ (((long) p_buffer[p_offset + 4] & 0xff) << 32) + (((long) p_buffer[p_offset + 5] & 0xff) << 40);
	}

	@Override
	public long getChunkID(final byte[] p_buffer, final int p_offset) {
		System.out.println("No ChunkID in NormalSecondaryLogEntryHeader!");
		return -1;
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
		System.out.println("********************Secondary Log Entry Header (Normal)********************");
		System.out.println("* LocalID: " + getLID(p_buffer, p_offset));
		System.out.println("* Length: " + getLength(p_buffer, p_offset));
		System.out.println("* Version: " + getVersion(p_buffer, p_offset));
		System.out.println("* Checksum: " + getChecksum(p_buffer, p_offset));
		System.out.println("***************************************************************************");
	}

	@Override
	public byte[] createHeader(final Chunk p_chunk, final byte p_rangeID, final short p_source) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte getRangeID(final byte[] p_buffer, final int p_offset) {
		System.out.println("No RangeID in NormalSecondaryLogEntryHeader!");
		return -1;
	}

	@Override
	public short getSource(final byte[] p_buffer, final int p_offset) {
		System.out.println("No source in NormalSecondaryLogEntryHeader!");
		return -1;
	}

	@Override
	public short getHeaderSize() {
		return SIZE;
	}

	@Override
	public short getRIDOffset() {
		System.out.println("No RangeID in NormalSecondaryLogEntryHeader!");
		return -1;
	}

	@Override
	public short getSRCOffset() {
		System.out.println("No source in NormalSecondaryLogEntryHeader!");
		return -1;
	}

	@Override
	public short getNIDOffset() {
		System.out.println("No NodeID in NormalSecondaryLogEntryHeader!");
		return -1;
	}

	@Override
	public short getLIDOffset() {
		return 0;
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
