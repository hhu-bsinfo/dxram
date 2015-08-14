
package de.uniduesseldorf.dxram.core.log.header;

import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.log.LogHandler;

/**
 * Implements a log entry header for removal (primary log)
 * @author Kevin Beineke
 *         25.06.2015
 */
public class MigrationPrimLogTombstone implements LogEntryHeaderInterface {
	// Attributes
	public static final short SIZE = 20;
	public static final byte RID_OFFSET = LogHandler.LOG_ENTRY_TYP_SIZE;
	public static final byte SRC_OFFSET = RID_OFFSET + LogHandler.LOG_ENTRY_RID_SIZE;
	public static final byte NID_OFFSET = SRC_OFFSET + LogHandler.LOG_ENTRY_SRC_SIZE;
	public static final byte LID_OFFSET = NID_OFFSET + LogHandler.LOG_ENTRY_NID_SIZE;
	public static final byte LEN_OFFSET = LID_OFFSET + LogHandler.LOG_ENTRY_LID_SIZE;
	public static final byte VER_OFFSET = LEN_OFFSET + LogHandler.LOG_ENTRY_LEN_SIZE;

	// Constructors
	/**
	 * Creates an instance of TombstonePrimaryLog
	 */
	public MigrationPrimLogTombstone() {}

	// Methods
	@Override
	public byte[] createHeader(final Chunk p_chunk, final byte p_rangeID, final short p_source) {
		byte[] result;

		result = new byte[SIZE];
		AbstractLogEntryHeader.putType(result, (byte) 3, (byte) 0);
		AbstractLogEntryHeader.putRangeID(result, p_rangeID, RID_OFFSET);
		AbstractLogEntryHeader.putSource(result, p_source, SRC_OFFSET);
		AbstractLogEntryHeader.putLength(result, 0, LEN_OFFSET);
		AbstractLogEntryHeader.putVersion(result, -1, VER_OFFSET);

		return result;
	}

	@Override
	public byte getRangeID(final byte[] p_buffer, final int p_offset, final boolean p_logStoresMigrations) {
		return p_buffer[RID_OFFSET];
	}

	@Override
	public short getSource(final byte[] p_buffer, final int p_offset, final boolean p_logStoresMigrations) {
		final int offset = p_offset + SRC_OFFSET;

		return (short) ((p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8));
	}

	@Override
	public short getNodeID(final byte[] p_buffer, final int p_offset, final boolean p_logStoresMigrations) {
		final int offset = p_offset + NID_OFFSET;

		return (short) ((p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8));
	}

	@Override
	public long getLID(final byte[] p_buffer, final int p_offset, final boolean p_logStoresMigrations) {
		final int offset = p_offset + LID_OFFSET;

		return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
				+ ((p_buffer[offset + 2] & 0xff) << 16) + (((long) p_buffer[offset + 3] & 0xff) << 24)
				+ (((long) p_buffer[offset + 4] & 0xff) << 32) + (((long) p_buffer[offset + 5] & 0xff) << 40);
	}

	@Override
	public long getChunkID(final byte[] p_buffer, final int p_offset, final boolean p_logStoresMigrations) {
		return ((long) getNodeID(p_buffer, p_offset, p_logStoresMigrations) << 48)
				+ getLID(p_buffer, p_offset, p_logStoresMigrations);
	}

	@Override
	public int getLength(final byte[] p_buffer, final int p_offset, final boolean p_logStoresMigrations) {
		final int offset = p_offset + LEN_OFFSET;

		return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
				+ ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24);
	}

	@Override
	public int getVersion(final byte[] p_buffer, final int p_offset, final boolean p_logStoresMigrations) {
		final int offset = p_offset + VER_OFFSET;

		return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
				+ ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24);
	}

	@Override
	public long getChecksum(final byte[] p_buffer, final int p_offset, final boolean p_logStoresMigrations) {
		System.out.println("No checksum available!");
		return -1;
	}

	@Override
	public short getHeaderSize(final boolean p_logStoresMigrations) {
		return SIZE;
	}

	@Override
	public short getConversionOffset(final boolean p_logStoresMigrations) {
		return getNIDOffset(p_logStoresMigrations);
	}

	@Override
	public short getRIDOffset(final boolean p_logStoresMigrations) {
		return RID_OFFSET;
	}

	@Override
	public short getSRCOffset(final boolean p_logStoresMigrations) {
		return SRC_OFFSET;
	}

	@Override
	public short getNIDOffset(final boolean p_logStoresMigrations) {
		return NID_OFFSET;
	}

	@Override
	public short getLIDOffset(final boolean p_logStoresMigrations) {
		return LID_OFFSET;
	}

	@Override
	public short getLENOffset(final boolean p_logStoresMigrations) {
		return LEN_OFFSET;
	}

	@Override
	public short getVEROffset(final boolean p_logStoresMigrations) {
		return VER_OFFSET;
	}

	@Override
	public short getCRCOffset(final boolean p_logStoresMigrations) {
		System.out.println("No checksum available!");
		return -1;
	}

	@Override
	public void print(final byte[] p_buffer, final int p_offset, final boolean p_logStoresMigrations) {
		System.out.println("********************TombstonePrimaryLog********************");
		System.out.println("* NodeID: " + getNodeID(p_buffer, p_offset, p_logStoresMigrations));
		System.out.println("* LocalID: " + getLID(p_buffer, p_offset, p_logStoresMigrations));
		System.out.println("* Length: " + getLength(p_buffer, p_offset, p_logStoresMigrations));
		System.out.println("* Version: " + getVersion(p_buffer, p_offset, p_logStoresMigrations));
		System.out.println("***********************************************************");
	}
}
