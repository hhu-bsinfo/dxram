/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.log.header;

import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.log.storage.Version;

/**
 * Class for efficiently accessing log entry headers.
 * Log entry headers are read and written with absolute methods (position is untouched), only!
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
    static final byte LOG_ENTRY_RID_SIZE = 2;
    static final byte LOG_ENTRY_OWN_SIZE = 2;
    private static final byte LOG_ENTRY_TSP_SIZE = 4;
    static final byte LOG_ENTRY_EPO_SIZE = 2;
    static final byte LOG_ENTRY_CHA_SIZE = 2;
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
    static int ms_timestampSize;

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
     * Returns the ChunkID or the LocalID of a log entry
     *
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the ChunkID if secondary log stores migrations, the LocalID otherwise
     */
    public abstract long getCID(final ByteBuffer p_buffer, final int p_offset);

    /**
     * Returns the ChunkID or the LocalID of a log entry (faster if type was already read)
     *
     * @param p_type
     *         the type field of the log entry
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the ChunkID if secondary log stores migrations, the LocalID otherwise
     */
    public abstract long getCID(final short p_type, final ByteBuffer p_buffer, final int p_offset);

    /**
     * Returns NodeID of a log entry
     *
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the NodeID
     */
    abstract short getNodeID(final ByteBuffer p_buffer, final int p_offset);

    /**
     * Returns the maximum log entry size
     *
     * @return the maximum log entry size
     */
    public static int getMaxLogEntrySize() {
        if (ms_segmentSize > 4 * 1024 * 1024) {
            return 2 * 1024 * 1024;
        } else {
            return ms_segmentSize / 2;
        }
    }

    /**
     * Sets the segment size
     *
     * @param p_segmentSize
     *         the segment size
     */
    public static void setSegmentSize(final int p_segmentSize) {
        ms_segmentSize = p_segmentSize;
    }

    /**
     * Sets the timestamp size
     *
     * @param p_useTimestamps
     *         whether timestamps are used or not
     */
    public static void setTimestampSize(final boolean p_useTimestamps) {
        if (p_useTimestamps) {
            ms_timestampSize = LOG_ENTRY_TSP_SIZE;
        } else {
            ms_timestampSize = 0;
        }
    }

    /**
     * Returns the number of bytes necessary to store given value
     *
     * @param p_localID
     *         the value
     * @return the number of bytes necessary to store given value
     */
    static byte getSizeForLocalIDField(final long p_localID) {
        byte ret;

        int bits = Long.SIZE - Long.numberOfLeadingZeros(p_localID);
        ret = (byte) ((bits % 8 != 0 ? 1 : 0) + bits / 8);

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
     *         the value
     * @return the number of bytes necessary to store given value
     */
    static byte getSizeForLengthField(final int p_length) {
        byte ret;

        int bits = (byte) (Integer.SIZE - Integer.numberOfLeadingZeros(p_length));
        ret = (byte) ((bits % 8 != 0 ? 1 : 0) + bits / 8);


        if (ret > 3) {
            LOGGER.error("Log Entry too large!");
        }


        return ret;
    }

    /**
     * Returns the number of bytes necessary to store given value
     *
     * @param p_version
     *         the value
     * @return the number of bytes necessary to store given value
     */
    static byte getSizeForVersionField(final int p_version) {
        byte ret = 0;

        if (p_version != 1) {
            int bits = (byte) (Integer.SIZE - Integer.numberOfLeadingZeros(p_version));
            ret = (byte) ((bits % 8 != 0 ? 1 : 0) + bits / 8);


            if (ret > 3) {
                LOGGER.error("Log Entry version too high!");
            }

        }

        return ret;
    }

    /**
     * Returns the type of a log entry
     *
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the type
     */
    private static short getType(final ByteBuffer p_buffer, final int p_offset) {
        return (short) (p_buffer.get(p_offset) & 0xFF);
    }

    /**
     * Checks whether this log entry is chained or not
     *
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return true if this log entry is one part of a large chunk
     */
    public final boolean isChained(final ByteBuffer p_buffer, final int p_offset) {
        return isChained(getType(p_buffer, p_offset));
    }

    /**
     * Checks whether this log entry is chained or not
     *
     * @param p_type
     *         the type field
     * @return true if this log entry is one part of a large chunk
     */
    public boolean isChained(final short p_type) {
        return (byte) (p_type & CHAIN_MASK) != 0;
    }

    /**
     * Returns the log entry header size
     *
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the size
     */
    public final short getHeaderSize(final ByteBuffer p_buffer, final int p_offset) {
        short ret;
        byte versionSize;
        final short type = getType(p_buffer, p_offset);

        if (ChecksumHandler.checksumsEnabled()) {
            ret = (short) (getCRCOffset(type) + ChecksumHandler.getCRCSize());
        } else {
            if (isChained(type)) {
                ret = (short) (getCHAOffset(type) + LOG_ENTRY_CHA_SIZE);
            } else {
                versionSize = (byte) (((type & VER_LENGTH_MASK) >> VER_LENGTH_SHFT) + LOG_ENTRY_EPO_SIZE);
                ret = (short) (getVEROffset(type) + versionSize);
            }
        }

        return ret;
    }

    /**
     * Returns the log entry header size. This is method is much faster than the one above but hard to read
     * because everything is inlined.
     *
     * @param p_type
     *         the type field of the log entry header
     * @return the size
     */
    public final short getHeaderSize(final short p_type) {
        short ret;

        // This is ugly but much faster as the JIT does not inline the methods in this class
        short lenOffset = getLIDOffset();
        switch ((byte) ((p_type & LID_LENGTH_MASK) >> LID_LENGTH_SHFT)) {
            case 0:
                lenOffset += 1;
                break;
            case 1:
                lenOffset += 2;
                break;
            case 2:
                lenOffset += 4;
                break;
            case 3:
                lenOffset += 6;
                break;
            default:

                LOGGER.error("LocalID's length unknown!");

                break;
        }

        short tspOffset = (short) (lenOffset + (byte) ((p_type & LEN_LENGTH_MASK) >> LEN_LENGTH_SHFT));
        short verOffset = (short) (tspOffset + ms_timestampSize);

        byte versionSize;
        if (ChecksumHandler.checksumsEnabled()) {
            versionSize = (byte) ((p_type & VER_LENGTH_MASK) >> VER_LENGTH_SHFT);
            short chaOffset = (short) (verOffset + LOG_ENTRY_EPO_SIZE + versionSize);

            final byte chainSize = isChained(p_type) ? LOG_ENTRY_CHA_SIZE : 0;
            short crcOffset = chaOffset;

            if (ChecksumHandler.checksumsEnabled()) {
                crcOffset += chainSize;
            } else {

                LOGGER.error("No checksum available!");

                crcOffset = -1;
            }

            ret = (short) (crcOffset + ChecksumHandler.getCRCSize());
        } else {
            if (isChained(p_type)) {
                versionSize = (byte) ((p_type & VER_LENGTH_MASK) >> VER_LENGTH_SHFT + LOG_ENTRY_EPO_SIZE);
                short chaOffset = (short) (verOffset + versionSize);

                ret = (short) (chaOffset + LOG_ENTRY_CHA_SIZE);
            } else {
                versionSize = (byte) (((p_type & VER_LENGTH_MASK) >> VER_LENGTH_SHFT) + LOG_ENTRY_EPO_SIZE);
                ret = (short) (verOffset + versionSize);
            }
        }

        return ret;
    }

    /**
     * Returns whether the length field is completely in this iteration or not
     *
     * @param p_type
     *         the type field of the log entry header
     * @param p_bytesUntilEnd
     *         number of bytes until wrap around
     * @return whether the length field is completely in this iteration or not
     */
    public final boolean isReadable(final short p_type, final int p_bytesUntilEnd) {
        // This is ugly but much faster as the JIT does not inline the methods in this class
        short lenOffset = getLIDOffset();
        switch ((byte) ((p_type & LID_LENGTH_MASK) >> LID_LENGTH_SHFT)) {
            case 0:
                lenOffset += 1;
                break;
            case 1:
                lenOffset += 2;
                break;
            case 2:
                lenOffset += 4;
                break;
            case 3:
                lenOffset += 6;
                break;
            default:

                LOGGER.error("LocalID's length unknown!");

                break;
        }

        short tspOffset = (short) (lenOffset + (byte) ((p_type & LEN_LENGTH_MASK) >> LEN_LENGTH_SHFT));

        return p_bytesUntilEnd >= (short) (tspOffset + ms_timestampSize);
    }

    /**
     * Returns length of a log entry
     *
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the length
     */
    final int getLength(final ByteBuffer p_buffer, final int p_offset) {
        return getLength(getType(p_buffer, p_offset), p_buffer, p_offset);

    }

    /**
     * Returns length of a log entry (faster if type was already read)
     *
     * @param p_type
     *         the type field of the log entry header
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the length
     */
    public final int getLength(final short p_type, final ByteBuffer p_buffer, final int p_offset) {
        int ret = 0;
        final int offset = p_offset + getLENOffset(p_type);
        final byte length = (byte) ((p_type & LEN_LENGTH_MASK) >> LEN_LENGTH_SHFT);

        if (length == 1) {
            ret = p_buffer.get(offset) & 0xFF;
        } else if (length == 2) {
            ret = p_buffer.getShort(offset) & 0xFFFF;
        } else if (length == 3) {
            ret = (p_buffer.get(offset) & 0xFF) + ((p_buffer.get(offset + 1) & 0xFF) << 8) +
                    ((p_buffer.get(offset + 2) & 0xFF) << 16);
        }

        return ret;
    }

    /**
     * Returns the timestamp of a log entry
     *
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the timestamp
     */
    final int getTimestamp(final ByteBuffer p_buffer, final int p_offset) {
        return getTimestamp(getType(p_buffer, p_offset), p_buffer, p_offset);
    }

    /**
     * Returns the timestamp of a log entry (faster if the type was already read)
     *
     * @param p_type
     *         the type field of the log entry
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the timestamp
     */
    public final int getTimestamp(final short p_type, final ByteBuffer p_buffer, final int p_offset) {
        int ret;
        int offset;

        if (ms_timestampSize != 0) {
            offset = p_offset + getTSPOffset(p_type);
            ret = p_buffer.getInt(offset);
        } else {

            LOGGER.error("No timestamp available!");

            ret = -1;
        }

        return ret;
    }

    /**
     * Returns epoch and version of a log entry
     *
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the epoch and version
     */
    public final Version getVersion(final ByteBuffer p_buffer, final int p_offset) {
        return getVersion(getType(p_buffer, p_offset), p_buffer, p_offset);
    }

    /**
     * Returns epoch and version of a log entry (faster if the type was already read)
     *
     * @param p_type
     *         the type field of the log entry
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the epoch and version
     */
    public final Version getVersion(final short p_type, final ByteBuffer p_buffer, final int p_offset) {
        final int offset = p_offset + getVEROffset(p_type);
        final byte length = (byte) ((p_type & VER_LENGTH_MASK) >> VER_LENGTH_SHFT);
        short epoch;
        int version = 1;

        epoch = p_buffer.getShort(offset);
        if (length == 1) {
            version = p_buffer.get(offset + LOG_ENTRY_EPO_SIZE) & 0xFF;
        } else if (length == 2) {
            version = p_buffer.getShort(offset + LOG_ENTRY_EPO_SIZE) & 0xFFFF;
        } else if (length == 3) {
            version = (p_buffer.get(offset + LOG_ENTRY_EPO_SIZE) & 0xFF) +
                    ((p_buffer.get(offset + LOG_ENTRY_EPO_SIZE + 1) & 0xFF) << 8) +
                    ((p_buffer.get(offset + LOG_ENTRY_EPO_SIZE + 2) & 0xFF) << 16);
        }

        return new Version(epoch, version);
    }

    /**
     * Returns the chaining ID of a log entry
     *
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the chaining ID
     */
    public final byte getChainID(final ByteBuffer p_buffer, final int p_offset) {
        return getChainID(getType(p_buffer, p_offset), p_buffer, p_offset);
    }

    /**
     * Returns the chaining ID of a log entry (faster if the type was already read)
     *
     * @param p_type
     *         the type field of the log entry
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the chaining ID
     */
    public final byte getChainID(final short p_type, final ByteBuffer p_buffer, final int p_offset) {
        byte ret;
        int offset;

        if (isChained(p_type)) {
            offset = p_offset + getCHAOffset(p_type);
            ret = p_buffer.get(offset);
        } else {

            LOGGER.error("Log entry is not chained!");

            ret = -1;
        }

        return ret;
    }

    /**
     * Returns the chain size of a log entry
     *
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the chain size
     */
    public final byte getChainSize(final ByteBuffer p_buffer, final int p_offset) {
        return getChainSize(getType(p_buffer, p_offset), p_buffer, p_offset);
    }

    /**
     * Returns the chain size of a log entry (faster if the type was already read)
     *
     * @param p_type
     *         the type field of the log entry
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the chain size
     */
    public final byte getChainSize(final short p_type, final ByteBuffer p_buffer, final int p_offset) {
        byte ret;
        int offset;

        if (isChained(p_type)) {
            offset = p_offset + getCHAOffset(p_type) + 1;
            ret = p_buffer.get(offset);
        } else {

            LOGGER.error("Log entry is not chained!");

            ret = -1;
        }

        return ret;
    }

    /**
     * Returns the checksum of a log entry's payload
     *
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the checksum
     */
    final int getChecksum(final ByteBuffer p_buffer, final int p_offset) {
        return getChecksum(getType(p_buffer, p_offset), p_buffer, p_offset);
    }

    /**
     * Returns the checksum of a log entry's payload (faster if the type was already read)
     *
     * @param p_type
     *         the type field of the log entry
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the checksum
     */
    public final int getChecksum(final short p_type, final ByteBuffer p_buffer, final int p_offset) {
        int ret;
        int offset;

        if (ChecksumHandler.checksumsEnabled()) {
            offset = p_offset + getCRCOffset(p_type);
            ret = p_buffer.getInt(offset);
        } else {

            LOGGER.error("No checksum available!");

            ret = -1;
        }

        return ret;
    }

    /**
     * Returns the LocalID
     *
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the LocalID
     */
    final long getLID(final ByteBuffer p_buffer, final int p_offset) {
        return getLID(getType(p_buffer, p_offset), p_buffer, p_offset);
    }

    /**
     * Returns the LocalID (faster if the type was already read)
     *
     * @param p_type
     *         the type field of the log entry
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the LocalID
     */
    long getLID(final short p_type, final ByteBuffer p_buffer, final int p_offset) {
        long ret = -1;
        final int offset = p_offset + getLIDOffset();
        final byte length = (byte) ((p_type & LID_LENGTH_MASK) >> LID_LENGTH_SHFT);

        if (length == 0) {
            ret = p_buffer.get(offset) & 0xFF;
        } else if (length == 1) {
            ret = p_buffer.getShort(offset) & 0xFFFF;
        } else if (length == 2) {
            ret = (long) p_buffer.getInt(offset) & 0xFFFFFFFFL;
        } else if (length == 3) {
            ret = (p_buffer.get(offset) & 0xFF) + ((p_buffer.get(offset + 1) & 0xFF) << 8) +
                    ((p_buffer.get(offset + 2) & 0xFF) << 16) + (((long) p_buffer.get(offset + 3) & 0xFF) << 24) +
                    (((long) p_buffer.get(offset + 4) & 0xFF) << 32) + (((long) p_buffer.get(offset + 5) & 0xFF) << 40);
        }

        return ret;
    }

    /**
     * Returns the length offset
     *
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the offset
     */
    final short getLENOffset(final ByteBuffer p_buffer, final int p_offset) {
        return getLENOffset(getType(p_buffer, p_offset));
    }

    /**
     * Returns the length offset
     *
     * @param p_type
     *         the type field
     * @return the offset
     */
    final short getLENOffset(final short p_type) {
        short ret = getLIDOffset();
        final byte localIDSize = (byte) ((p_type & LID_LENGTH_MASK) >> LID_LENGTH_SHFT);

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

                LOGGER.error("LocalID's length unknown!");

                break;
        }

        return ret;
    }

    /**
     * Returns the timestamp offset
     *
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the offset
     */
    private short getTSPOffset(final ByteBuffer p_buffer, final int p_offset) {
        return getTSPOffset(getType(p_buffer, p_offset));
    }

    /**
     * Returns the timestamp offset
     *
     * @param p_type
     *         the type field
     * @return the offset
     */
    final short getTSPOffset(final short p_type) {
        final short ret = getLENOffset(p_type);
        final byte lengthSize = (byte) ((p_type & LEN_LENGTH_MASK) >> LEN_LENGTH_SHFT);

        return (short) (ret + lengthSize);
    }

    /**
     * Returns the version offset
     *
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the offset
     */
    final short getVEROffset(final ByteBuffer p_buffer, final int p_offset) {
        return getVEROffset(getType(p_buffer, p_offset));
    }

    /**
     * Returns the version offset
     *
     * @param p_type
     *         the type field
     * @return the offset
     */
    final short getVEROffset(final short p_type) {
        return (short) (getTSPOffset(p_type) + ms_timestampSize);
    }

    /**
     * Returns the chaining ID offset
     *
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the offset
     */
    final short getCHAOffset(final ByteBuffer p_buffer, final int p_offset) {
        return getCHAOffset(getType(p_buffer, p_offset));
    }

    /**
     * Returns the chaining ID offset
     *
     * @param p_type
     *         the type field
     * @return the offset
     */
    private short getCHAOffset(final short p_type) {
        short ret = (short) (getVEROffset(p_type) + LOG_ENTRY_EPO_SIZE);
        final byte versionSize = (byte) ((p_type & VER_LENGTH_MASK) >> VER_LENGTH_SHFT);

        return (short) (ret + versionSize);
    }

    /**
     * Returns the checksum offset
     *
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the offset
     */
    final short getCRCOffset(final ByteBuffer p_buffer, final int p_offset) {
        return getCRCOffset(getType(p_buffer, p_offset));
    }

    /**
     * Returns the checksum offset
     *
     * @param p_type
     *         the type field
     * @return the offset
     */
    private short getCRCOffset(final short p_type) {
        short ret = getCHAOffset(p_type);
        final byte chainSize = isChained(p_type) ? LOG_ENTRY_CHA_SIZE : 0;

        if (ChecksumHandler.checksumsEnabled()) {
            ret += chainSize;
        } else {

            LOGGER.error("No checksum available!");

            ret = -1;
        }

        return ret;
    }

    /**
     * Generates the type field containing the information:
     * - migrated or not
     * - chained or not
     * - local ID size
     * - length size
     * - version size
     * The type field cannot be 0 because at least the length field is not 0
     *
     * @param p_type
     *         the type of the log entry header
     * @param p_localIDSize
     *         the size of the checksum field
     * @param p_lengthSize
     *         the size of the length field
     * @param p_versionSize
     *         the size of the version field
     * @param p_needsChaining
     *         whether this chunk is too large and must be split or not
     * @return the type field
     */
    final byte generateTypeField(final byte p_type, final byte p_localIDSize, final byte p_lengthSize,
            final byte p_versionSize, final boolean p_needsChaining) {
        byte ret = p_type;

        // Chaining field present or not
        ret |= p_needsChaining ? 2 : 0;

        // Length of LocalID is between 0 and 6 Bytes, but there are only 2 Bits for storing the length. Different
        // lengths: 1, 2, 4 ,6
        switch (p_localIDSize) {
            case 1:
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

                LOGGER.error("Unknown LocalID!");

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
     * @param p_buffer
     *         the buffer to write into
     * @param p_type
     *         the type (0 => normal, 1 => migration)
     */
    final void putType(final ByteBuffer p_buffer, final byte p_type) {
        p_buffer.put(0, p_type);
    }

    /**
     * Puts RangeID of log entry in log entry header
     *
     * @param p_buffer
     *         the buffer to write into
     * @param p_rangeID
     *         the RangeID
     * @param p_offset
     *         the type-specific offset
     */
    final void putRangeID(final ByteBuffer p_buffer, final short p_rangeID, final int p_offset) {
        p_buffer.putShort(p_offset, p_rangeID);
    }

    /**
     * Puts source of log entry in log entry header
     *
     * @param p_buffer
     *         the buffer to write into
     * @param p_source
     *         the source
     * @param p_offset
     *         the type-specific offset
     */
    final void putOwner(final ByteBuffer p_buffer, final short p_source, final int p_offset) {
        p_buffer.putShort(p_offset, p_source);
    }

    /**
     * Puts ChunkID in log entry header
     *
     * @param p_buffer
     *         the buffer to write into
     * @param p_chunkID
     *         the ChunkID
     * @param p_localIDSize
     *         the length of the LocalID field
     * @param p_offset
     *         the type-specific offset
     */
    final void putChunkID(final ByteBuffer p_buffer, final long p_chunkID, final byte p_localIDSize,
            final int p_offset) {
        // NodeID
        p_buffer.putShort(p_offset, ChunkID.getCreatorID(p_chunkID));

        // LocalID
        switch (p_localIDSize) {
            case 1:
                p_buffer.put(p_offset + LOG_ENTRY_NID_SIZE, (byte) p_chunkID);
                break;
            case 2:
                p_buffer.putShort(p_offset + LOG_ENTRY_NID_SIZE, (short) p_chunkID);
                break;
            case 4:
                p_buffer.putInt(p_offset + LOG_ENTRY_NID_SIZE, (int) p_chunkID);
                break;
            case 6:
                for (int i = 0; i < p_localIDSize; i++) {
                    p_buffer.put(p_offset + LOG_ENTRY_NID_SIZE + i, (byte) (p_chunkID >> i * 8 & 0xFF));
                }
                break;
            default:
                break;
        }
    }

    /**
     * Puts length of log entry in log entry header
     *
     * @param p_buffer
     *         the buffer to write into
     * @param p_length
     *         the length
     * @param p_offset
     *         the type-specific offset
     */
    final void putLength(final ByteBuffer p_buffer, final byte p_length, final int p_offset) {
        p_buffer.put(p_offset, (byte) (p_length & 0xFF));
    }

    /**
     * Puts length of log entry in log entry header
     *
     * @param p_buffer
     *         the buffer to write into
     * @param p_length
     *         the length
     * @param p_offset
     *         the type-specific offset
     */
    final void putLength(final ByteBuffer p_buffer, final short p_length, final int p_offset) {
        p_buffer.putShort(p_offset, p_length);
    }

    /**
     * Puts length of log entry in log entry header
     *
     * @param p_buffer
     *         the buffer to write into
     * @param p_length
     *         the length
     * @param p_offset
     *         the type-specific offset
     */
    final void putLength(final ByteBuffer p_buffer, final int p_length, final int p_offset) {
        for (int i = 0; i < MAX_LOG_ENTRY_LEN_SIZE; i++) {
            p_buffer.put(p_offset + i, (byte) (p_length >> i * 8 & 0xFF));
        }
    }

    /**
     * Puts timestamp of log entry in log entry header
     *
     * @param p_buffer
     *         the buffer to write into
     * @param p_timestamp
     *         the timestamp
     * @param p_offset
     *         the type-specific offset
     */
    final void putTimestamp(final ByteBuffer p_buffer, final int p_timestamp, final int p_offset) {
        p_buffer.putInt(p_offset, p_timestamp);
    }

    /**
     * Puts epoch of log entry in log entry header
     *
     * @param p_buffer
     *         the buffer to write into
     * @param p_epoch
     *         the version
     * @param p_offset
     *         the type-specific offset
     */
    final void putEpoch(final ByteBuffer p_buffer, final short p_epoch, final int p_offset) {
        p_buffer.putShort(p_offset, p_epoch);
    }

    /**
     * Puts version of log entry in log entry header
     *
     * @param p_buffer
     *         the buffer to write into
     * @param p_version
     *         the version
     * @param p_offset
     *         the type-specific offset
     */
    final void putVersion(final ByteBuffer p_buffer, final byte p_version, final int p_offset) {
        p_buffer.put(p_offset, (byte) (p_version & 0xFF));
    }

    /**
     * Puts version of log entry in log entry header
     *
     * @param p_buffer
     *         the buffer to write into
     * @param p_version
     *         the version
     * @param p_offset
     *         the type-specific offset
     */
    final void putVersion(final ByteBuffer p_buffer, final short p_version, final int p_offset) {
        p_buffer.putShort(p_offset, p_version);
    }

    /**
     * Puts version of log entry in log entry header
     *
     * @param p_buffer
     *         the buffer to write into
     * @param p_version
     *         the version
     * @param p_offset
     *         the type-specific offset
     */
    final void putVersion(final ByteBuffer p_buffer, final int p_version, final int p_offset) {
        for (int i = 0; i < MAX_LOG_ENTRY_VER_SIZE; i++) {
            p_buffer.put(p_offset + i, (byte) (p_version >> i * 8 & 0xFF));
        }
    }

    /**
     * Puts chaining ID of log entry in log entry header
     *
     * @param p_buffer
     *         the buffer to write into
     * @param p_chainingID
     *         the version
     * @param p_offset
     *         the type-specific offset
     */
    final void putChainingID(final ByteBuffer p_buffer, final byte p_chainingID, final int p_offset) {
        p_buffer.put(p_offset, (byte) (p_chainingID & 0xFF));
    }

}
