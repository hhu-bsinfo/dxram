/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.log.header;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.log.storage.Version;

/**
 * Extends AbstractLogEntryHeader for a migration log entry header (primary log)
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 25.06.2015
 */
public class MigrationPrimLogEntryHeader extends AbstractLogEntryHeader {

    private static final Logger LOGGER = LogManager.getFormatterLogger(MigrationPrimLogEntryHeader.class.getSimpleName());

    // Attributes
    private static short ms_maximumSize;
    private static byte ms_typeOffset;
    private static byte ms_ridOffset;
    private static byte ms_srcOffset;
    private static byte ms_nidOffset;
    private static byte ms_lidOffset;

    // Constructors

    /**
     * Creates an instance of MigrationLogEntryHeader
     */
    public MigrationPrimLogEntryHeader() {
        ms_maximumSize =
            (short) (LOG_ENTRY_TYP_SIZE + LOG_ENTRY_RID_SIZE + LOG_ENTRY_SRC_SIZE + MAX_LOG_ENTRY_CID_SIZE + LOG_ENTRY_EPO_SIZE + MAX_LOG_ENTRY_LEN_SIZE +
                MAX_LOG_ENTRY_VER_SIZE + AbstractLogEntryHeader.getCRCSize());
        ms_typeOffset = 0;
        ms_ridOffset = LOG_ENTRY_TYP_SIZE;
        ms_srcOffset = (byte) (ms_ridOffset + LOG_ENTRY_RID_SIZE);
        ms_nidOffset = (byte) (ms_srcOffset + LOG_ENTRY_SRC_SIZE);
        ms_lidOffset = (byte) (ms_nidOffset + LOG_ENTRY_NID_SIZE);
    }

    @Override
    public short getMaxHeaderSize() {
        return ms_maximumSize;
    }

    @Override
    public short getConversionOffset() {
        return getNIDOffset();
    }

    @Override
    protected short getNIDOffset() {
        return ms_nidOffset;
    }

    @Override
    protected short getLIDOffset() {
        return ms_lidOffset;
    }

    // Methods
    @Override
    public byte[] createLogEntryHeader(final long p_chunkID, final int p_size, final Version p_version, final byte p_rangeID, final short p_source) {
        byte[] result;
        byte lengthSize;
        byte localIDSize;
        byte versionSize;
        byte checksumSize = 0;
        byte type = 2;

        localIDSize = getSizeForLocalIDField(ChunkID.getLocalID(p_chunkID));
        lengthSize = getSizeForLengthField(p_size);
        versionSize = getSizeForVersionField(p_version.getVersion());

        if (AbstractLogEntryHeader.useChecksum()) {
            checksumSize = AbstractLogEntryHeader.getCRCSize();
        }

        type = generateTypeField(type, localIDSize, lengthSize, versionSize);

        result = new byte[ms_lidOffset + localIDSize + lengthSize + LOG_ENTRY_EPO_SIZE + versionSize + checksumSize];

        putType(result, type, ms_typeOffset);
        putRangeID(result, p_rangeID, ms_ridOffset);
        putSource(result, p_source, ms_srcOffset);

        putChunkID(result, p_chunkID, localIDSize, ms_nidOffset);

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

        return result;
    }

    @Override
    public short getType(final byte[] p_buffer, final int p_offset) {
        return (short) (p_buffer[p_offset] & 0x00FF);
    }

    @Override
    public byte getRangeID(final byte[] p_buffer, final int p_offset) {
        return p_buffer[p_offset + ms_ridOffset];
    }

    @Override
    public short getSource(final byte[] p_buffer, final int p_offset) {
        final int offset = p_offset + ms_srcOffset;

        return (short) ((p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8));
    }

    @Override
    public short getNodeID(final byte[] p_buffer, final int p_offset) {
        final int offset = p_offset + ms_nidOffset;

        return (short) ((p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8));
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
            ret = (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8) + ((p_buffer[offset + 2] & 0xff) << 16);
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
            version = (p_buffer[offset + LOG_ENTRY_EPO_SIZE] & 0xff) + ((p_buffer[offset + LOG_ENTRY_EPO_SIZE + 1] & 0xff) << 8) +
                ((p_buffer[offset + LOG_ENTRY_EPO_SIZE + 2] & 0xff) << 16);
        }

        return new Version(epoch, version);
    }

    @Override
    public int getChecksum(final byte[] p_buffer, final int p_offset) {
        int ret;
        int offset;

        if (AbstractLogEntryHeader.useChecksum()) {
            offset = p_offset + getCRCOffset(p_buffer, p_offset);
            ret = (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8) + ((p_buffer[offset + 2] & 0xff) << 16) +
                ((p_buffer[offset + 3] & 0xff) << 24);
        } else {
            // #if LOGGER >= ERROR
            LOGGER.error("No checksum available!");
            // #endif /* LOGGER >= ERROR */
            ret = -1;
        }

        return ret;
    }

    @Override
    public boolean wasMigrated() {
        return true;
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
    public boolean readable(final byte[] p_buffer, final int p_offset, final int p_bytesUntilEnd) {
        return p_bytesUntilEnd >= getVEROffset(p_buffer, p_offset);
    }

    @Override
    public void print(final byte[] p_buffer, final int p_offset) {
        final Version version = getVersion(p_buffer, p_offset);

        System.out.println("********************Primary Log Entry Header (Migration)********************");
        System.out.println("* NodeID: " + getNodeID(p_buffer, p_offset));
        System.out.println("* LocalID: " + getLID(p_buffer, p_offset));
        System.out.println("* Length: " + getLength(p_buffer, p_offset));
        System.out.println("* Version: " + version.getEpoch() + ", " + version.getVersion());
        if (AbstractLogEntryHeader.useChecksum()) {
            System.out.println("* Checksum: " + getChecksum(p_buffer, p_offset));
        }
        System.out.println("****************************************************************************");
    }

    @Override
    protected short getLENOffset(final byte[] p_buffer, final int p_offset) {
        short ret = ms_lidOffset;
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
                LOGGER.error("LocalID's length unknown!");
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
            LOGGER.error("No checksum available!");
            // #endif /* LOGGER >= ERROR */
            ret = -1;
        }

        return ret;
    }

    /**
     * Returns the LocalID
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the LocalID
     */
    private long getLID(final byte[] p_buffer, final int p_offset) {
        long ret = -1;
        final int offset = p_offset + ms_lidOffset;
        final byte length = (byte) ((getType(p_buffer, p_offset) & LID_LENGTH_MASK) >> LID_LENGTH_SHFT);

        if (length == 0) {
            ret = p_buffer[offset] & 0xff;
        } else if (length == 1) {
            ret = (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8);
        } else if (length == 2) {
            ret = (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8) + ((p_buffer[offset + 2] & 0xff) << 16) +
                ((p_buffer[offset + 3] & 0xff) << 24);
        } else if (length == 3) {
            ret = (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8) + ((p_buffer[offset + 2] & 0xff) << 16) +
                (((long) p_buffer[offset + 3] & 0xff) << 24) + (((long) p_buffer[offset + 4] & 0xff) << 32) + (((long) p_buffer[offset + 5] & 0xff) << 40);
        }

        return ret;
    }
}
