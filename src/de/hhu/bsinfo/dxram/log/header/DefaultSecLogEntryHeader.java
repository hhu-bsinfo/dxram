
package de.hhu.bsinfo.dxram.log.header;

import de.hhu.bsinfo.dxram.log.storage.Version;

/**
 * Extends AbstractLogEntryHeader for a normal log entry header (secondary log)
 * @author Kevin Beineke
 *         25.06.2015
 */
public class DefaultSecLogEntryHeader extends AbstractLogEntryHeader {

	// Attributes
	private static short m_maximumSize;
	private static byte m_lidOffset;

	// Constructors
	/**
	 * Creates an instance of NormalSecondaryLogEntryHeader
	 */
	public DefaultSecLogEntryHeader() {
		m_maximumSize = (short) (LOG_ENTRY_TYP_SIZE + MAX_LOG_ENTRY_LID_SIZE + MAX_LOG_ENTRY_LEN_SIZE
				+ LOG_ENTRY_EPO_SIZE + MAX_LOG_ENTRY_VER_SIZE + AbstractLogEntryHeader.getCRCSize());
		m_lidOffset = LOG_ENTRY_TYP_SIZE;
	}

	// Methods
	@Override
	public byte[] createLogEntryHeader(final long p_chunkID, final int p_size, final Version p_version, final byte p_rangeID, final short p_source) {
		// #if LOGGER >= WARN
		AbstractLogEntryHeader.getLogger().warn(DefaultSecLogEntryHeader.class,
				"Do not call createLogEntryHeader() for secondary log entries. Convert instead.");
		// #endif /* LOGGER >= WARN */
		return null;
	}

	@Override
	public short getType(final byte[] p_buffer, final int p_offset) {
		return (short) (p_buffer[p_offset] & 0x00FF);
	}

	@Override
	public byte getRangeID(final byte[] p_buffer, final int p_offset) {
		// #if LOGGER >= ERROR
		AbstractLogEntryHeader.getLogger().error(DefaultSecLogEntryHeader.class, "No RangeID available!");
		// #endif /* LOGGER >= ERROR */
		return -1;
	}

	@Override
	public short getSource(final byte[] p_buffer, final int p_offset) {
		// #if LOGGER >= ERROR
		AbstractLogEntryHeader.getLogger().error(DefaultSecLogEntryHeader.class, "No source available!");
		// #endif /* LOGGER >= ERROR */
		return -1;
	}

	@Override
	public short getNodeID(final byte[] p_buffer, final int p_offset) {
		// #if LOGGER >= ERROR
		AbstractLogEntryHeader.getLogger().error(DefaultSecLogEntryHeader.class, "No NodeID available!");
		// #endif /* LOGGER >= ERROR */
		return -1;
	}

	/**
	 * Returns the LocalID
	 * @param p_buffer
	 *            buffer with log entries
	 * @param p_offset
	 *            offset in buffer
	 * @return the LocalID
	 */
	public long getLID(final byte[] p_buffer, final int p_offset) {
		long ret = -1;
		final int offset = p_offset + m_lidOffset;
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
		return getLID(p_buffer, p_offset);
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
	public Version getVersion(final byte[] p_buffer, final int p_offset) {
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

		return new Version(epoch, version);
	}

	@Override
	public int getChecksum(final byte[] p_buffer, final int p_offset) {
		int ret;
		int offset;

		if (AbstractLogEntryHeader.useChecksum()) {
			offset = p_offset + getCRCOffset(p_buffer, p_offset);
			ret = (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8) + ((p_buffer[offset + 2] & 0xff) << 16)
					+ ((p_buffer[offset + 3] & 0xff) << 24);
		} else {
			// #if LOGGER >= ERROR
			AbstractLogEntryHeader.getLogger().error(DefaultSecLogEntryHeader.class, "No checksum available!");
			// #endif /* LOGGER >= ERROR */
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

		if (AbstractLogEntryHeader.useChecksum()) {
			ret = (short) (getCRCOffset(p_buffer, p_offset) + AbstractLogEntryHeader.getCRCSize());
		} else {
			versionSize = (byte) (((getType(p_buffer, p_offset) & VER_LENGTH_MASK) >> VER_LENGTH_SHFT) + LOG_ENTRY_EPO_SIZE);
			ret = (short) (getVEROffset(p_buffer, p_offset) + versionSize);
		}

		return ret;
	}

	@Override
	public short getMaxHeaderSize() {
		return m_maximumSize;
	}

	@Override
	public short getConversionOffset() {
		// #if LOGGER >= ERROR
		AbstractLogEntryHeader.getLogger().error(DefaultSecLogEntryHeader.class, "No conversion offset available!");
		// #endif /* LOGGER >= ERROR */
		return -1;
	}

	@Override
	public boolean readable(final byte[] p_buffer, final int p_offset, final int p_bytesUntilEnd) {
		return p_bytesUntilEnd >= getVEROffset(p_buffer, p_offset);
	}

	@Override
	protected short getNIDOffset() {
		// #if LOGGER >= ERROR
		AbstractLogEntryHeader.getLogger().error(DefaultSecLogEntryHeader.class, "No NodeID available!");
		// #endif /* LOGGER >= ERROR */
		return -1;
	}

	@Override
	protected short getLIDOffset() {
		return m_lidOffset;
	}

	@Override
	protected short getLENOffset(final byte[] p_buffer, final int p_offset) {
		short ret = m_lidOffset;
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
			// #if LOGGER >= ERROR
			AbstractLogEntryHeader.getLogger().error(DefaultSecLogEntryHeader.class, "LocalID's lenght unknown!");
			// #endif /* LOGGER >= ERROR */
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
		short ret = (short) (getVEROffset(p_buffer, p_offset) + LOG_ENTRY_EPO_SIZE);
		final byte versionSize = (byte) ((getType(p_buffer, p_offset) & VER_LENGTH_MASK) >> VER_LENGTH_SHFT);

		if (AbstractLogEntryHeader.useChecksum()) {
			ret += versionSize;
		} else {
			// #if LOGGER >= ERROR
			AbstractLogEntryHeader.getLogger().error(DefaultSecLogEntryHeader.class, "No checksum available!");
			// #endif /* LOGGER >= ERROR */
			ret = -1;
		}

		return ret;
	}

	@Override
	public void print(final byte[] p_buffer, final int p_offset) {
		final Version version = getVersion(p_buffer, p_offset);

		System.out.println("********************Secondary Log Entry Header (Normal)********************");
		System.out.println("* LocalID: " + getLID(p_buffer, p_offset));
		System.out.println("* Length: " + getLength(p_buffer, p_offset));
		System.out.println("* Version: " + version.getEpoch() + ", " + version.getVersion());
		if (AbstractLogEntryHeader.useChecksum()) {
			System.out.println("* Checksum: " + getChecksum(p_buffer, p_offset));
		}
		System.out.println("***************************************************************************");
	}
}
