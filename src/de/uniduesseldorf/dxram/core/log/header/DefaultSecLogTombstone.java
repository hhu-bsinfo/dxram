
package de.uniduesseldorf.dxram.core.log.header;

import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.log.LogHandler;

/**
 * Implements a log entry header for removal (secondary log)
 * @author Kevin Beineke
 *         25.06.2015
 */
public class DefaultSecLogTombstone implements LogEntryHeaderInterface {

	// Attributes
	public static final short SIZE = 10;
	public static final byte LID_OFFSET = 0;
	public static final byte VER_OFFSET = LogHandler.LOG_ENTRY_LID_SIZE;

	// Constructors
	/**
	 * Creates an instance of TombstoneSecondaryLog
	 */
	public DefaultSecLogTombstone() {}

	// Methods
	@Override
	public byte[] createHeader(final Chunk p_chunk, final byte p_rangeID, final short p_source) {
		System.out.println("Do not call createHeader() for secondary log entries. Convert instead.");

		return null;
	}

	@Override
	public byte getRangeID(final byte[] p_buffer, final int p_offset) {
		System.out.println("No RangeID available!");
		return -1;
	}

	@Override
	public short getSource(final byte[] p_buffer, final int p_offset) {
		System.out.println("No source available!");
		return -1;
	}

	@Override
	public short getNodeID(final byte[] p_buffer, final int p_offset) {
		System.out.println("No NodeID available!");
		return -1;
	}

	@Override
	public long getLID(final byte[] p_buffer, final int p_offset) {
		return (p_buffer[p_offset] & 0xff) + ((p_buffer[p_offset + 1] & 0xff) << 8) + ((p_buffer[p_offset + 2] & 0xff) << 16)
				+ (((long) p_buffer[p_offset + 3] & 0xff) << 24) + (((long) p_buffer[p_offset + 4] & 0xff) << 32)
				+ (((long) p_buffer[p_offset + 5] & 0xff) << 40);
	}

	@Override
	public long getChunkID(final byte[] p_buffer, final int p_offset) {
		System.out.println("No ChunkID available!");
		return -1;
	}

	@Override
	public int getLength(final byte[] p_buffer, final int p_offset) {
		return 0;
	}

	@Override
	public int getVersion(final byte[] p_buffer, final int p_offset) {
		final int offset = p_offset + VER_OFFSET;

		return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8) + ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24);
	}

	@Override
	public long getChecksum(final byte[] p_buffer, final int p_offset) {
		System.out.println("No checksum available!");
		return -1;
	}

	@Override
	public short getHeaderSize() {
		return SIZE;
	}

	@Override
	public short getConversionOffset() {
		System.out.println("No conversion offset available!");
		return -1;
	}

	@Override
	public short getRIDOffset() {
		System.out.println("No RangeID available!");
		return -1;
	}

	@Override
	public short getSRCOffset() {
		System.out.println("No source available!");
		return -1;
	}

	@Override
	public short getNIDOffset() {
		System.out.println("No NodeID available!");
		return -1;
	}

	@Override
	public short getLIDOffset() {
		return LID_OFFSET;
	}

	@Override
	public short getLENOffset() {
		System.out.println("No length available!");
		return -1;
	}

	@Override
	public short getVEROffset() {
		return VER_OFFSET;
	}

	@Override
	public short getCRCOffset() {
		System.out.println("No checksum available!");
		return -1;
	}

	@Override
	public void print(final byte[] p_buffer, final int p_offset) {
		System.out.println("********************Tombstone for Secondary Log********************");
		System.out.println("* LocalID: " + getLID(p_buffer, p_offset));
		System.out.println("* Length: " + getLength(p_buffer, p_offset));
		System.out.println("* Version: " + getVersion(p_buffer, p_offset));
		System.out.println("*******************************************************************");
	}
}
