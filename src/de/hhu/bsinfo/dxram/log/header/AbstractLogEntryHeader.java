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
 * Class for efficiently accessing log entry headers.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 25.01.2017
 */
@SuppressWarnings("MethodMayBeStatic")
public abstract class AbstractLogEntryHeader {

    // Constants
    static final byte LOG_ENTRY_TYP_SIZE = 1;
    static final byte LOG_ENTRY_NID_SIZE = 2;
    static final byte MAX_LOG_ENTRY_LID_SIZE = 6;
    static final byte MAX_LOG_ENTRY_CID_SIZE = LOG_ENTRY_NID_SIZE + MAX_LOG_ENTRY_LID_SIZE;
    static final byte MAX_LOG_ENTRY_LEN_SIZE = 3;
    static final byte MAX_LOG_ENTRY_VER_SIZE = 3;
    static final byte LOG_ENTRY_RID_SIZE = 1;
    static final byte LOG_ENTRY_SRC_SIZE = 2;
    static final byte LOG_ENTRY_EPO_SIZE = 2;
    static final byte TYPE_MASK = (byte) 0x01;
    private static final byte CHAIN_MASK = (byte) 0x02;
    private static final byte LID_LENGTH_MASK = (byte) 0x0C;
    private static final byte LEN_LENGTH_MASK = (byte) 0x30;
    private static final byte VER_LENGTH_MASK = (byte) 0xC0;
    private static final byte LID_LENGTH_SHFT = (byte) 2;
    private static final byte LEN_LENGTH_SHFT = (byte) 4;
    private static final byte VER_LENGTH_SHFT = (byte) 6;
    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractLogEntryHeader.class.getSimpleName());

    private static int ms_segmentSize;

    /**
     * Returns the NodeID offset
     *
     * @return the offset
     */
    abstract short getNIDOffset();

    /**
     * Returns the LocalID offset
     *
     * @return the offset
     */
    abstract short getLIDOffset();

    /**
     * Returns the maximum log entry size
     *
     * @return the maximum log entry size
     */
    public static int getMaxLogEntrySize() {
        if (ms_segmentSize >= 4) {
            return 2 * 1024 * 1024;
        } else {
            return ms_segmentSize / 2;
        }
    }

    /**
     * Sets the segment size
     *
     * @param p_segmentSize
     *     the segment size
     */
    public static void setSegmentSize(final int p_segmentSize) {
        ms_segmentSize = p_segmentSize;
    }

    /**
     * Returns the ChunkID or the LocalID of a log entry
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the ChunkID if secondary log stores migrations, the LocalID otherwise
     */
    public abstract long getCID(final byte[] p_buffer, final int p_offset);

    /**
     * Returns NodeID of a log entry
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the NodeID
     */
    abstract short getNodeID(final byte[] p_buffer, final int p_offset);

    /**
     * Returns the number of bytes necessary to store given value
     *
     * @param p_localID
     *     the value
     * @return the number of bytes necessary to store given value
     */
    static byte getSizeForLocalIDField(final long p_localID) {
        byte ret;

        ret = (byte) Math.ceil(Math.log10(p_localID + 1) / Math.log10(2) / 8);

        // Only allow sizes 1, 2, 4 and 6, because there are only four states
        if (ret == 0) {
            ret = 1;
        } else if (ret == 3) {
            ret = 4;
        } else if (ret == 5) {
            ret = 6;
        }

        return ret;
    }

    /**
     * Returns the number of bytes necessary to store given value
     *
     * @param p_length
     *     the value
     * @return the number of bytes necessary to store given value
     */
    static byte getSizeForLengthField(final int p_length) {
        byte ret;

        ret = (byte) Math.ceil(Math.log10(p_length + 1) / Math.log10(2) / 8);

        // #if LOGGER >= ERROR
        if (ret > 3) {
            LOGGER.error("Log Entry too large!");
        }
        // #endif /* LOGGER >= ERROR */

        return ret;
    }

    /**
     * Returns the number of bytes necessary to store given value
     *
     * @param p_version
     *     the value
     * @return the number of bytes necessary to store given value
     */
    static byte getSizeForVersionField(final int p_version) {
        byte ret = 0;

        if (p_version != 1) {
            ret = (byte) Math.ceil(Math.log10(p_version + 1) / Math.log10(2) / 8);

            // #if LOGGER >= ERROR
            if (ret > 3) {
                LOGGER.error("Log Entry version too high!");
            }
            // #endif /* LOGGER >= ERROR */
        }

        return ret;
    }

    /**
     * Returns the type of a log entry
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the type
     */
    private static short getType(final byte[] p_buffer, final int p_offset) {
        return (short) (p_buffer[p_offset] & 0x00FF);
    }

    /**
     * Checks whether this log entry is chained or not
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return true if this log entry is one part of a large chunk
     */
    public boolean isChained(final byte[] p_buffer, final int p_offset) {
        boolean ret = false;
        byte type;

        type = (byte) (p_buffer[p_offset] & CHAIN_MASK);
        return type == 1;
    }

    /**
     * Returns the log entry header size
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the size
     */
    public short getHeaderSize(final byte[] p_buffer, final int p_offset) {
        short ret;
        byte versionSize;

        if (ChecksumHandler.checksumsEnabled()) {
            ret = (short) (getCRCOffset(p_buffer, p_offset) + ChecksumHandler.getCRCSize());
        } else {
            versionSize = (byte) (((getType(p_buffer, p_offset) & VER_LENGTH_MASK) >> VER_LENGTH_SHFT) + LOG_ENTRY_EPO_SIZE);
            ret = (short) (getVEROffset(p_buffer, p_offset) + versionSize);
        }

        return ret;
    }

    /**
     * Returns whether the length field is completely in this iteration or not
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @param p_bytesUntilEnd
     *     number of bytes until wrap around
     * @return whether the length field is completely in this iteration or not
     */
    public boolean isReadable(final byte[] p_buffer, final int p_offset, final int p_bytesUntilEnd) {
        return p_bytesUntilEnd >= getVEROffset(p_buffer, p_offset);
    }

    /**
     * Returns length of a log entry
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the length
     */
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

    /**
     * Returns epoch and version of a log entry
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the epoch and version
     */
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

    /**
     * Returns the chaining ID of a log entry
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the chaining ID
     */
    public byte getChainID(final byte[] p_buffer, final int p_offset) {
        byte ret;
        int offset;

        if (isChained(p_buffer, p_offset)) {
            offset = p_offset + getCHAOffset(p_buffer, p_offset);
            ret = p_buffer[offset];
        } else {
            // #if LOGGER >= ERROR
            LOGGER.error("Log entry is not chained!");
            // #endif /* LOGGER >= ERROR */
            ret = -1;
        }

        return ret;
    }

    /**
     * Returns the checksum of a log entry's payload
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the checksum
     */
    public int getChecksum(final byte[] p_buffer, final int p_offset) {
        int ret;
        int offset;

        if (ChecksumHandler.checksumsEnabled()) {
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

    /**
     * Returns the LocalID
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the LocalID
     */
    long getLID(final byte[] p_buffer, final int p_offset) {
        long ret = -1;
        final int offset = p_offset + getLIDOffset();
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

    /**
     * Returns the length offset
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the offset
     */
    short getLENOffset(final byte[] p_buffer, final int p_offset) {
        short ret = getLIDOffset();
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

    /**
     * Returns the version offset
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the offset
     */
    short getVEROffset(final byte[] p_buffer, final int p_offset) {
        final short ret = getLENOffset(p_buffer, p_offset);
        final byte lengthSize = (byte) ((getType(p_buffer, p_offset) & LEN_LENGTH_MASK) >> LEN_LENGTH_SHFT);

        return (short) (ret + lengthSize);
    }

    /**
     * Returns the chaining ID offset
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the offset
     */
    short getCHAOffset(final byte[] p_buffer, final int p_offset) {
        short ret = (short) (getVEROffset(p_buffer, p_offset) + LOG_ENTRY_EPO_SIZE);
        final byte versionSize = (byte) ((getType(p_buffer, p_offset) & VER_LENGTH_MASK) >> VER_LENGTH_SHFT);

        return (short) (ret + versionSize);
    }

    /**
     * Returns the checksum offset
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the offset
     */
    short getCRCOffset(final byte[] p_buffer, final int p_offset) {
        short ret = getCHAOffset(p_buffer, p_offset);
        final byte chainSize = (byte) (isChained(p_buffer, p_offset) ? 1 : 0);

        if (ChecksumHandler.checksumsEnabled()) {
            ret += chainSize;
        } else {
            // #if LOGGER >= ERROR
            LOGGER.error("No checksum available!");
            // #endif /* LOGGER >= ERROR */
            ret = -1;
        }

        return ret;
    }

    /**
     * Returns the number of bytes necessary to store given value
     *
     * @param p_type
     *     the type of the log entry header
     * @param p_localIDSize
     *     the size of the checksum field
     * @param p_lengthSize
     *     the size of the length field
     * @param p_versionSize
     *     the size of the version field
     * @param p_needsChaining
     *     whether this chunk is too large and must be split or not
     * @return the number of bytes necessary to store given value
     */
    byte generateTypeField(final byte p_type, final byte p_localIDSize, final byte p_lengthSize, final byte p_versionSize, final boolean p_needsChaining) {
        byte ret = p_type;

        // Chaining field present or not
        ret |= p_needsChaining ? 2 : 0;

        // Length of LocalID is between 0 and 6 Bytes, but there are only 2 Bits for storing the length. Different
        // lengths: 1, 2, 4 ,6
        switch (p_localIDSize) {
            case 1:
                ret |= 0;
                break;
            case 2:
                ret |= 1 << LID_LENGTH_SHFT;
                break;
            case 4:
                ret |= 2 << LID_LENGTH_SHFT;
                break;
            case 6:
                ret |= 3 << LID_LENGTH_SHFT;
                break;
            default:
                // #if LOGGER >= ERROR
                LOGGER.error("Unknown LocalID!");
                // #endif /* LOGGER >= ERROR */
                break;
        }

        // Length of size is linear: 0, 1, 2, 3
        ret |= p_lengthSize << LEN_LENGTH_SHFT;

        // Length of version is linear, too: 0, 1, 2, 3
        ret |= p_versionSize << VER_LENGTH_SHFT;

        return ret;
    }

    /**
     * Puts type of log entry in log entry header
     *
     * @param p_logEntry
     *     log entry
     * @param p_type
     *     the type (0 => normal, 1 => migration)
     * @param p_offset
     *     the type-specific offset
     */
    void putType(final byte[] p_logEntry, final byte p_type, final short p_offset) {
        p_logEntry[p_offset] = p_type;
    }

    /**
     * Puts RangeID of log entry in log entry header
     *
     * @param p_logEntry
     *     log entry
     * @param p_rangeID
     *     the RangeID
     * @param p_offset
     *     the type-specific offset
     */
    void putRangeID(final byte[] p_logEntry, final byte p_rangeID, final short p_offset) {
        p_logEntry[p_offset] = p_rangeID;
    }

    /**
     * Puts source of log entry in log entry header
     *
     * @param p_logEntry
     *     log entry
     * @param p_source
     *     the source
     * @param p_offset
     *     the type-specific offset
     */
    void putSource(final byte[] p_logEntry, final short p_source, final short p_offset) {
        for (int i = 0; i < LOG_ENTRY_SRC_SIZE; i++) {
            p_logEntry[p_offset + i] = (byte) (p_source >> i * 8 & 0xff);
        }
    }

    /**
     * Puts ChunkID in log entry header
     *
     * @param p_logEntry
     *     log entry
     * @param p_chunkID
     *     the ChunkID
     * @param p_localIDSize
     *     the length of the LocalID field
     * @param p_offset
     *     the type-specific offset
     */
    void putChunkID(final byte[] p_logEntry, final long p_chunkID, final byte p_localIDSize, final short p_offset) {
        // NodeID
        for (int i = 0; i < LOG_ENTRY_NID_SIZE; i++) {
            p_logEntry[p_offset + i] = (byte) (ChunkID.getCreatorID(p_chunkID) >> i * 8 & 0xff);
        }

        // LocalID
        for (int i = 0; i < p_localIDSize; i++) {
            p_logEntry[p_offset + LOG_ENTRY_NID_SIZE + i] = (byte) (p_chunkID >> i * 8 & 0xff);
        }
    }

    /**
     * Puts length of log entry in log entry header
     *
     * @param p_logEntry
     *     log entry
     * @param p_length
     *     the length
     * @param p_offset
     *     the type-specific offset
     */
    void putLength(final byte[] p_logEntry, final byte p_length, final short p_offset) {
        p_logEntry[p_offset] = (byte) (p_length & 0xff);
    }

    /**
     * Puts length of log entry in log entry header
     *
     * @param p_logEntry
     *     log entry
     * @param p_length
     *     the length
     * @param p_offset
     *     the type-specific offset
     */
    void putLength(final byte[] p_logEntry, final short p_length, final short p_offset) {
        for (int i = 0; i < Short.BYTES; i++) {
            p_logEntry[p_offset + i] = (byte) (p_length >> i * 8 & 0xff);
        }
    }

    /**
     * Puts length of log entry in log entry header
     *
     * @param p_logEntry
     *     log entry
     * @param p_length
     *     the length
     * @param p_offset
     *     the type-specific offset
     */
    void putLength(final byte[] p_logEntry, final int p_length, final short p_offset) {
        for (int i = 0; i < MAX_LOG_ENTRY_LEN_SIZE; i++) {
            p_logEntry[p_offset + i] = (byte) (p_length >> i * 8 & 0xff);
        }
    }

    /**
     * Puts epoch of log entry in log entry header
     *
     * @param p_logEntry
     *     log entry
     * @param p_epoch
     *     the version
     * @param p_offset
     *     the type-specific offset
     */
    void putEpoch(final byte[] p_logEntry, final short p_epoch, final short p_offset) {
        for (int i = 0; i < Short.BYTES; i++) {
            p_logEntry[p_offset + i] = (byte) (p_epoch >> i * 8 & 0xff);
        }
    }

    /**
     * Puts version of log entry in log entry header
     *
     * @param p_logEntry
     *     log entry
     * @param p_version
     *     the version
     * @param p_offset
     *     the type-specific offset
     */
    void putVersion(final byte[] p_logEntry, final byte p_version, final short p_offset) {
        p_logEntry[p_offset] = (byte) (p_version & 0xff);
    }

    /**
     * Puts version of log entry in log entry header
     *
     * @param p_logEntry
     *     log entry
     * @param p_version
     *     the version
     * @param p_offset
     *     the type-specific offset
     */
    void putVersion(final byte[] p_logEntry, final short p_version, final short p_offset) {
        for (int i = 0; i < Short.BYTES; i++) {
            p_logEntry[p_offset + i] = (byte) (p_version >> i * 8 & 0xff);
        }
    }

    /**
     * Puts version of log entry in log entry header
     *
     * @param p_logEntry
     *     log entry
     * @param p_version
     *     the version
     * @param p_offset
     *     the type-specific offset
     */
    void putVersion(final byte[] p_logEntry, final int p_version, final short p_offset) {
        for (int i = 0; i < MAX_LOG_ENTRY_VER_SIZE; i++) {
            p_logEntry[p_offset + i] = (byte) (p_version >> i * 8 & 0xff);
        }
    }

    /**
     * Puts chaining ID of log entry in log entry header
     *
     * @param p_logEntry
     *     log entry
     * @param p_chainingID
     *     the version
     * @param p_offset
     *     the type-specific offset
     */
    void putChainingID(final byte[] p_logEntry, final byte p_chainingID, final short p_offset) {
        p_logEntry[p_offset] = (byte) (p_chainingID & 0xff);
    }

}
