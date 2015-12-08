
package de.uniduesseldorf.dxram.core.log.header;

import de.uniduesseldorf.dxram.core.log.EpochVersion;
import de.uniduesseldorf.dxram.core.util.ChunkID;

/**
 * Extends AbstractLogEntryHeader for a normal log entry header (primary log)
 * @author Kevin Beineke
 *         25.06.2015
 */
public class DefaultPrimLogEntryHeader extends AbstractLogEntryHeader {

	// Attributes
	private static final short MAX_SIZE = (short) (LOG_ENTRY_TYP_SIZE + MAX_LOG_ENTRY_CID_SIZE + MAX_LOG_ENTRY_LEN_SIZE
			+ MAX_LOG_ENTRY_VER_SIZE + LOG_ENTRY_CRC_SIZE);
	private static final byte TYP_OFFSET = 0;
	private static final byte NID_OFFSET = LOG_ENTRY_TYP_SIZE;
	private static final byte LID_OFFSET = NID_OFFSET + LOG_ENTRY_NID_SIZE;

	// Constructors
	/**
	 * Creates an instance of NormalLogEntryHeader
	 */
	public DefaultPrimLogEntryHeader() {};

	// Methods
	@Override
	public byte[] createLogEntryHeader(final long p_chunkID, final int p_size, final EpochVersion p_version,
			final byte[] p_data, final byte p_rangeID, final short p_source) {
		byte[] result;
		byte lengthSize;
		byte localIDSize;
		byte versionSize;
		byte checksumSize = 0;
		byte type = 0;

		localIDSize = getSizeForLocalIDField(ChunkID.getLocalID(p_chunkID));
		lengthSize = getSizeForLengthField(p_size);
		versionSize = (byte) (getSizeForVersionField(p_version.getVersion()) + LOG_ENTRY_EPO_SIZE);

		if (USE_CHECKSUM) {
			checksumSize = LOG_ENTRY_CRC_SIZE;
		}

		type = generateTypeField(type, localIDSize, lengthSize, versionSize);

		result = new byte[LID_OFFSET + localIDSize + lengthSize + versionSize + checksumSize];

		putType(result, type, TYP_OFFSET);

		putChunkID(result, p_chunkID, localIDSize, NID_OFFSET);

		if (lengthSize == 1) {
			putLength(result, (byte) p_size, getLENOffset(result, 0));
		} else if (lengthSize == 2) {
			putLength(result, (short) p_size, getLENOffset(result, 0));
		} else {
			putLength(result, p_size, getLENOffset(result, 0));
		}

		putEpoch(result, p_version.getEpoch(), getVEROffset(result, 0));
		if (versionSize == 1) {
			putVersion(result, (byte) p_version.getVersion(), (short) (getVEROffset(result, 0) + LOG_ENTRY_EPO_SIZE));
		} else if (versionSize == 2) {
			putVersion(result, (short) p_version.getVersion(), (short) (getVEROffset(result, 0) + LOG_ENTRY_EPO_SIZE));
		} else if (versionSize > 2) {
			putVersion(result, p_version.getVersion(), (short) (getVEROffset(result, 0) + LOG_ENTRY_EPO_SIZE));
		}

		if (checksumSize > 0) {
			putChecksum(result, calculateChecksumOfPayload(p_data), getCRCOffset(result, 0));
		}

		return result;
	}

	@Override
	protected short getType(final byte[] p_buffer, final int p_offset) {
		return (short) (p_buffer[p_offset] & 0x00FF);
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

	/**
	 * Returns the LocalID
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the LocalID
	 */
	private long getLID(final byte[] p_buffer, final int p_offset) {
		long ret = -1;
		final int offset = p_offset + LID_OFFSET;
		final byte length = (byte) ((getType(p_buffer, p_offset) & LID_LENGTH_MASK) >> LID_LENGTH_SHFT);

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
	public long getCID(final byte[] p_buffer, final int p_offset) {
		return ((long) getNodeID(p_buffer, p_offset) << 48) + getLID(p_buffer, p_offset);
	}

	@Override
	public int getLength(final byte[] p_buffer, final int p_offset) {
		int ret = 0;
		final int offset = p_offset + getLENOffset(p_buffer, p_offset);
		final byte length = (byte) ((getType(p_buffer, p_offset) & LEN_LENGTH_MASK) >> LEN_LENGTH_SHFT);

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
	public EpochVersion getVersion(final byte[] p_buffer, final int p_offset) {
		final int offset = p_offset + getVEROffset(p_buffer, p_offset);
		final byte length = (byte) ((getType(p_buffer, p_offset) & VER_LENGTH_MASK) >> VER_LENGTH_SHFT);
		short epoch;
		int version = 1;

		epoch = (short) ((p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8));
		if (length == 1) {
			version = p_buffer[offset + LOG_ENTRY_EPO_SIZE] & 0xff;
		} else if (length == 2) {
			version = (p_buffer[offset + LOG_ENTRY_EPO_SIZE] & 0xff) + ((p_buffer[offset + LOG_ENTRY_EPO_SIZE + 1] & 0xff) << 8);
		} else if (length == 3) {
			version = (p_buffer[offset + LOG_ENTRY_EPO_SIZE] & 0xff) + ((p_buffer[offset + LOG_ENTRY_EPO_SIZE + 1] & 0xff) << 8)
					+ ((p_buffer[offset + LOG_ENTRY_EPO_SIZE + 2] & 0xff) << 16);
		}

		return new EpochVersion(epoch, version);
	}

	@Override
	public int getChecksum(final byte[] p_buffer, final int p_offset) {
		int ret;
		int offset;

		if (USE_CHECKSUM) {
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
		return false;
	}

	@Override
	public short getHeaderSize(final byte[] p_buffer, final int p_offset) {
		short ret;
		byte versionSize;

		if (USE_CHECKSUM) {
			ret = (short) (getCRCOffset(p_buffer, p_offset) + LOG_ENTRY_CRC_SIZE);
		} else {
			versionSize = (byte) ((getType(p_buffer, p_offset) & VER_LENGTH_MASK) >> VER_LENGTH_SHFT);
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
		return getLIDOffset();
	}

	@Override
	public boolean readable(final byte[] p_buffer, final int p_offset, final int p_bytesUntilEnd) {
		return p_bytesUntilEnd >= getVEROffset(p_buffer, p_offset);
	}

	@Override
	protected short getNIDOffset() {
		return NID_OFFSET;
	}

	@Override
	protected short getLIDOffset() {
		return LID_OFFSET;
	}

	@Override
	protected short getLENOffset(final byte[] p_buffer, final int p_offset) {
		short ret = LID_OFFSET;
		final byte localIDSize = (byte) ((getType(p_buffer, p_offset) & LID_LENGTH_MASK) >> LID_LENGTH_SHFT);

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
	protected short getVEROffset(final byte[] p_buffer, final int p_offset) {
		final short ret = getLENOffset(p_buffer, p_offset);
		final byte lengthSize = (byte) ((getType(p_buffer, p_offset) & LEN_LENGTH_MASK) >> LEN_LENGTH_SHFT);

		return (short) (ret + lengthSize);
	}

	@Override
	protected short getCRCOffset(final byte[] p_buffer, final int p_offset) {
		short ret = getVEROffset(p_buffer, p_offset);
		final byte versionSize = (byte) ((getType(p_buffer, p_offset) & VER_LENGTH_MASK) >> VER_LENGTH_SHFT);

		if (USE_CHECKSUM) {
			ret += versionSize;
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
		if (USE_CHECKSUM) {
			System.out.println("* Checksum: " + getChecksum(p_buffer, p_offset));
		}
		System.out.println("************************************************************");
	}
}
