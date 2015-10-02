
package de.uniduesseldorf.dxram.core.log.header;

import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.log.LogHandler;

/**
 * Implements a normal log entry header
 * @author Kevin Beineke
 *         25.06.2015
 */
public class DefaultPrimLogEntryHeader implements LogEntryHeaderInterface {

	// Attributes
	public static final short MAX_SIZE = 25;
	public static final byte TYP_OFFSET = 0;
	public static final byte NID_OFFSET = LogHandler.LOG_ENTRY_TYP_SIZE;
	public static final byte LID_OFFSET = NID_OFFSET + LogHandler.LOG_ENTRY_NID_SIZE;
	public static final byte LEN_OFFSET = LID_OFFSET + LogHandler.LOG_ENTRY_LID_SIZE;
	public static final byte DEF_VER_OFFSET = LEN_OFFSET + LogHandler.DEF_LOG_ENTRY_LEN_SIZE;
	public static final byte DEF_CRC_OFFSET = DEF_VER_OFFSET + LogHandler.DEF_LOG_ENTRY_VER_SIZE;

	public static final byte LEN_LENGTH_MASK = (byte) 0x0000FF00;
	public static final byte VER_LENGTH_MASK = (byte) 0x00FF0000;
	public static final byte CRC_LENGTH_MASK = (byte) 0xFF000000;

	// Constructors
	/**
	 * Creates an instance of NormalLogEntryHeader
	 */
	public DefaultPrimLogEntryHeader() {};

	// Methods
	@Override
	public byte[] createLogEntryHeader(final Chunk p_chunk, final byte p_rangeID, final short p_source) {
		byte[] result;
		byte lengthSize;
		byte versionSize;
		byte checksumSize;
		byte type = 0;

		lengthSize = (byte) (Math.ceil(Math.log10(p_chunk.getSize()) / Math.log10(2)) / 16); // TODO: Use negative versions
		versionSize = (byte) (Math.ceil(Math.log10(p_chunk.getVersion()) / Math.log10(2)) / 16);
		checksumSize = (byte) 0;

		type |= lengthSize << 2;
		type |= versionSize << 4;
		type |= checksumSize << 6;

		result = new byte[LEN_OFFSET + lengthSize + versionSize + checksumSize];

		AbstractLogEntryHeader.putType(result, type, TYP_OFFSET);

		AbstractLogEntryHeader.putChunkID(result, p_chunk.getChunkID(), NID_OFFSET);

		if (lengthSize == 1) {
			AbstractLogEntryHeader.putLength(result, (byte) p_chunk.getSize(), LEN_OFFSET);
		} else if (lengthSize == 2) {
			AbstractLogEntryHeader.putLength(result, (short) p_chunk.getSize(), LEN_OFFSET);
		} else {
			AbstractLogEntryHeader.putLength(result, p_chunk.getSize(), LEN_OFFSET);
		}

		if (versionSize == 1) {
			AbstractLogEntryHeader.putVersion(result, (byte) p_chunk.getVersion(), getVEROffset(result, 0));
		} else if (versionSize == 2) {
			AbstractLogEntryHeader.putVersion(result, (short) p_chunk.getVersion(), getVEROffset(result, 0));
		} else {
			AbstractLogEntryHeader.putVersion(result, p_chunk.getVersion(), getVEROffset(result, 0));
		}

		if (checksumSize > 0) {
			AbstractLogEntryHeader.putChecksum(result, AbstractLogEntryHeader.calculateChecksumOfPayload(p_chunk.getData().array()), getCRCOffset(result, 0));
		}

		return result;
	}

	@Override
	public byte[] createTombstone(final long p_chunkID, final int p_version, final byte p_rangeID, final short p_source) {
		System.out.println("This is not a tombstone!");
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
		final int offset = p_offset + NID_OFFSET;

		return (short) ((p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8));
	}

	@Override
	public long getLID(final byte[] p_buffer, final int p_offset) {
		final int offset = p_offset + LID_OFFSET;

		return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8) + ((p_buffer[offset + 2] & 0xff) << 16)
				+ (((long) p_buffer[offset + 3] & 0xff) << 24) + (((long) p_buffer[offset + 4] & 0xff) << 32) + (((long) p_buffer[offset + 5] & 0xff) << 40);
	}

	@Override
	public long getChunkID(final byte[] p_buffer, final int p_offset) {
		return ((long) getNodeID(p_buffer, p_offset) << 48) + getLID(p_buffer, p_offset);
	}

	@Override
	public int getLength(final byte[] p_buffer, final int p_offset) {
		final int offset = p_offset + LEN_OFFSET;

		return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8) + ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24);
	}

	@Override
	public int getVersion(final byte[] p_buffer, final int p_offset) {
		final int offset = p_offset + getVEROffset(p_buffer, p_offset);

		return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8) + ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24);
	}

	@Override
	public long getChecksum(final byte[] p_buffer, final int p_offset) {
		long ret;
		short type;
		int offset;

		type = (short) ((short) p_buffer[p_offset] & 0xff);
		if ((type & CRC_LENGTH_MASK) != 0) {
			offset = p_offset + getCRCOffset(p_buffer, p_offset);
			ret = (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8) + ((p_buffer[offset + 2] & 0xff) << 16)
					+ ((p_buffer[offset + 3] & 0xff) << 24) + ((p_buffer[offset + 4] & 0xff) << 32)
					+ ((p_buffer[offset + 5] & 0xff) << 40) + ((p_buffer[offset + 6] & 0xff) << 48)
					+ ((p_buffer[offset + 7] & 0xff) << 54);
		} else {
			System.out.println("No checksum available!");
			ret = -1;
		}

		return ret;
	}

	@Override
	public short getHeaderSize(final byte[] p_buffer, final int p_offset) {
		short ret = LEN_OFFSET;
		short type;

		type = (short) ((short) p_buffer[p_offset] & 0xff);

		ret += type & LEN_LENGTH_MASK;
		ret += type & LEN_LENGTH_MASK;
		if ((type & CRC_LENGTH_MASK) != 0) {
			ret += LogHandler.DEF_LOG_ENTRY_VER_SIZE;
		}

		return ret;
	}

	@Override
	public short getMaxHeaderSize() {
		return MAX_SIZE;
	}

	@Override
	public short getConversionOffset() {
		return getLIDOffset();
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
	public short getVEROffset(final byte[] p_buffer, final int p_offset) {
		short ret = DEF_VER_OFFSET;
		short type;

		type = (short) ((short) p_buffer[p_offset] & 0xff);

		ret += type & LEN_LENGTH_MASK;

		return ret;
	}

	@Override
	public short getCRCOffset(final byte[] p_buffer, final int p_offset) {
		short ret = DEF_VER_OFFSET;
		short type;

		type = (short) ((short) p_buffer[p_offset] & 0xff);
		if ((type & CRC_LENGTH_MASK) != 0) {
			ret += type & LEN_LENGTH_MASK;
			ret += type & VER_LENGTH_MASK;
		} else {
			System.out.println("No checksum available!");
			ret = -1;
		}

		return ret;
	}

	@Override
	public void print(final byte[] p_buffer, final int p_offset) {
		System.out.println("********************Primary Log Entry Header (Normal)*******");
		System.out.println("* NodeID: " + getNodeID(p_buffer, p_offset));
		System.out.println("* LocalID: " + getLID(p_buffer, p_offset));
		System.out.println("* Length: " + getLength(p_buffer, p_offset));
		System.out.println("* Version: " + getVersion(p_buffer, p_offset));
		System.out.println("* Checksum: " + getChecksum(p_buffer, p_offset));
		System.out.println("************************************************************");
	}
}
