
package de.uniduesseldorf.dxram.core.log.header;

import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.log.LogHandler;

/**
 * Implements a log entry header for migration
 * @author Kevin Beineke
 *         25.06.2015
 */
public class MigrationPrimLogEntryHeader implements LogEntryHeaderInterface {

	// Attributes
	private static final short MAX_SIZE = (short) (LogHandler.LOG_ENTRY_TYP_SIZE + LogHandler.LOG_ENTRY_RID_SIZE + LogHandler.LOG_ENTRY_SRC_SIZE
			+ LogHandler.MAX_LOG_ENTRY_CID_SIZE + LogHandler.MAX_LOG_ENTRY_LEN_SIZE + LogHandler.MAX_LOG_ENTRY_VER_SIZE + LogHandler.LOG_ENTRY_CRC_SIZE);
	private static final byte TYP_OFFSET = 0;
	private static final byte RID_OFFSET = LogHandler.LOG_ENTRY_TYP_SIZE;
	private static final byte SRC_OFFSET = RID_OFFSET + LogHandler.LOG_ENTRY_RID_SIZE;
	private static final byte NID_OFFSET = SRC_OFFSET + LogHandler.LOG_ENTRY_SRC_SIZE;
	private static final byte LID_OFFSET = NID_OFFSET + LogHandler.LOG_ENTRY_NID_SIZE;

	// Constructors
	/**
	 * Creates an instance of MigrationLogEntryHeader
	 */
	public MigrationPrimLogEntryHeader() {}

	// Methods
	@Override
	public byte[] createLogEntryHeader(final Chunk p_chunk, final byte p_rangeID, final short p_source) {
		byte[] result;
		byte lengthSize;
		byte localIDSize;
		byte versionSize;
		byte checksumSize = 0;
		byte type = 2;

		localIDSize = AbstractLogEntryHeader.getSizeForLocalIDField(ChunkID.getLocalID(p_chunk.getChunkID()));
		lengthSize = AbstractLogEntryHeader.getSizeForLengthField(p_chunk.getSize());
		versionSize = AbstractLogEntryHeader.getSizeForVersionField(p_chunk.getVersion());

		if (LogHandler.USE_CHECKSUM) {
			checksumSize = LogHandler.LOG_ENTRY_CRC_SIZE;
		}

		type = AbstractLogEntryHeader.generateTypeField(type, localIDSize, lengthSize, versionSize);

		result = new byte[LID_OFFSET + localIDSize + lengthSize + versionSize + checksumSize];

		AbstractLogEntryHeader.putType(result, type, TYP_OFFSET);
		AbstractLogEntryHeader.putRangeID(result, p_rangeID, RID_OFFSET);
		AbstractLogEntryHeader.putSource(result, p_source, SRC_OFFSET);

		AbstractLogEntryHeader.putChunkID(result, p_chunk.getChunkID(), localIDSize, NID_OFFSET);

		if (lengthSize == 1) {
			AbstractLogEntryHeader.putLength(result, (byte) p_chunk.getSize(), getLENOffset(result, 0));
		} else if (lengthSize == 2) {
			AbstractLogEntryHeader.putLength(result, (short) p_chunk.getSize(), getLENOffset(result, 0));
		} else {
			AbstractLogEntryHeader.putLength(result, p_chunk.getSize(), getLENOffset(result, 0));
		}

		if (versionSize == 1) {
			AbstractLogEntryHeader.putVersion(result, (byte) p_chunk.getVersion(), getVEROffset(result, 0));
		} else if (versionSize == 2) {
			AbstractLogEntryHeader.putVersion(result, (short) p_chunk.getVersion(), getVEROffset(result, 0));
		} else if (versionSize > 2) {
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
	public short getType(final byte[] p_buffer, final int p_offset) {
		return (short) (p_buffer[p_offset] & 0x00FF);
	}

	@Override
	public byte getRangeID(final byte[] p_buffer, final int p_offset) {
		return p_buffer[p_offset + RID_OFFSET];
	}

	@Override
	public short getSource(final byte[] p_buffer, final int p_offset) {
		final int offset = p_offset + SRC_OFFSET;

		return (short) ((p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8));
	}

	@Override
	public short getNodeID(final byte[] p_buffer, final int p_offset) {
		final int offset = p_offset + NID_OFFSET;

		return (short) ((p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8));
	}

	@Override
	public long getLID(final byte[] p_buffer, final int p_offset) {
		long ret = -1;
		final int offset = p_offset + LID_OFFSET;
		final byte length = (byte) ((getType(p_buffer, p_offset) & AbstractLogEntryHeader.LID_LENGTH_MASK) >> AbstractLogEntryHeader.LID_LENGTH_SHFT);

		if (length == 0) {
			ret = p_buffer[offset] & 0xff;
		} else if (length == 1) {
			ret = (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8);
		} else if (length == 2) {
			ret = (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
					+ ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24);
		} else if (length == 3) {
			ret = (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8) + ((p_buffer[offset + 2] & 0xff) << 16)
					+ (((long) p_buffer[offset + 3] & 0xff) << 24) + (((long) p_buffer[offset + 4] & 0xff) << 32)
					+ (((long) p_buffer[offset + 5] & 0xff) << 40);
		}

		return ret;
	}

	@Override
	public long getChunkID(final byte[] p_buffer, final int p_offset) {
		return ((long) getNodeID(p_buffer, p_offset) << 48) + getLID(p_buffer, p_offset);
	}

	@Override
	public int getLength(final byte[] p_buffer, final int p_offset) {
		int ret = 0;
		final int offset = p_offset + getLENOffset(p_buffer, p_offset);
		final byte length = (byte) ((getType(p_buffer, p_offset) & AbstractLogEntryHeader.LEN_LENGTH_MASK) >> AbstractLogEntryHeader.LEN_LENGTH_SHFT);

		if (length == 1) {
			ret = p_buffer[offset] & 0xff;
		} else if (length == 2) {
			ret = (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8);
		} else if (length == 3) {
			ret = (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
					+ ((p_buffer[offset + 2] & 0xff) << 16);
		}

		return ret;
	}

	@Override
	public int getVersion(final byte[] p_buffer, final int p_offset) {
		int ret = 1;
		final int offset = p_offset + getVEROffset(p_buffer, p_offset);
		final byte length = (byte) ((getType(p_buffer, p_offset) & AbstractLogEntryHeader.VER_LENGTH_MASK) >> AbstractLogEntryHeader.VER_LENGTH_SHFT);

		if (length == 1) {
			ret = p_buffer[offset] & 0xff;
		} else if (length == 2) {
			ret = (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8);
		} else if (length == 3) {
			ret = (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
					+ ((p_buffer[offset + 2] & 0xff) << 16);
		}

		return ret;
	}

	@Override
	public long getChecksum(final byte[] p_buffer, final int p_offset) {
		long ret;
		int offset;

		if (LogHandler.USE_CHECKSUM) {
			offset = p_offset + getCRCOffset(p_buffer, p_offset);
			ret = (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8) + ((p_buffer[offset + 2] & 0xff) << 16)
					+ ((p_buffer[offset + 3] & 0xff) << 24);
		} else {
			System.out.println("No checksum available!");
			ret = -1;
		}

		return ret;
	}

	@Override
	public boolean wasMigrated() {
		return true;
	}

	@Override
	public boolean isTombstone() {
		return false;
	}

	@Override
	public boolean isInvalid(final byte[] p_buffer, final int p_offset) {
		return false;
	}

	@Override
	public short getHeaderSize(final byte[] p_buffer, final int p_offset) {
		short ret;
		byte versionSize;

		if (LogHandler.USE_CHECKSUM) {
			ret = (short) (getCRCOffset(p_buffer, p_offset) + LogHandler.LOG_ENTRY_CRC_SIZE);
		} else {
			versionSize = (byte) ((getType(p_buffer, p_offset) & AbstractLogEntryHeader.VER_LENGTH_MASK) >> AbstractLogEntryHeader.VER_LENGTH_SHFT);
			ret = (short) (getVEROffset(p_buffer, p_offset) + versionSize);
		}

		return ret;
	}

	@Override
	public short getMaxHeaderSize() {
		return MAX_SIZE;
	}

	@Override
	public short getConversionOffset() {
		return getNIDOffset();
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
	public short getLENOffset(final byte[] p_buffer, final int p_offset) {
		short ret = LID_OFFSET;
		final byte localIDSize = (byte) ((getType(p_buffer, p_offset) & AbstractLogEntryHeader.LID_LENGTH_MASK) >> AbstractLogEntryHeader.LID_LENGTH_SHFT);

		switch (localIDSize) {
		case 0:
			ret += 1;
			break;
		case 1:
			ret += 2;
			break;
		case 2:
			ret += 4;
			break;
		case 3:
			ret += 6;
			break;
		default:
			System.out.println("Error: LocalID length unknown!");
			break;
		}

		return ret;
	}

	@Override
	public short getVEROffset(final byte[] p_buffer, final int p_offset) {
		final short ret = getLENOffset(p_buffer, p_offset);
		final byte lengthSize = (byte) ((getType(p_buffer, p_offset) & AbstractLogEntryHeader.LEN_LENGTH_MASK) >> AbstractLogEntryHeader.LEN_LENGTH_SHFT);

		return (short) (ret + lengthSize);
	}

	@Override
	public short getCRCOffset(final byte[] p_buffer, final int p_offset) {
		short ret = getVEROffset(p_buffer, p_offset);
		final byte versionSize = (byte) ((getType(p_buffer, p_offset) & AbstractLogEntryHeader.VER_LENGTH_MASK) >> AbstractLogEntryHeader.VER_LENGTH_SHFT);

		if (LogHandler.USE_CHECKSUM) {
			ret += versionSize;
		} else {
			System.out.println("No checksum available!");
			ret = -1;
		}

		return ret;
	}

	@Override
	public void print(final byte[] p_buffer, final int p_offset) {
		System.out.println("********************Primary Log Entry Header (Migration)********************");
		System.out.println("* NodeID: " + getNodeID(p_buffer, p_offset));
		System.out.println("* LocalID: " + getLID(p_buffer, p_offset));
		System.out.println("* Length: " + getLength(p_buffer, p_offset));
		System.out.println("* Version: " + getVersion(p_buffer, p_offset));
		if (LogHandler.USE_CHECKSUM) {
			System.out.println("* Checksum: " + getChecksum(p_buffer, p_offset));
		}
		System.out.println("****************************************************************************");
	}
}
