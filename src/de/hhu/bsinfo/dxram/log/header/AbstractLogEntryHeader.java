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

import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.log.storage.Version;

/**
 * A helper class for the LogEntryHeaderInterface.
 * Provides methods to detect the type of a log entry header or to convert one.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 25.06.2015
 */
public abstract class AbstractLogEntryHeader {

    static final byte LID_LENGTH_MASK = (byte) 0x0C;
    static final byte LEN_LENGTH_MASK = (byte) 0x30;
    static final byte VER_LENGTH_MASK = (byte) 0xC0;
    static final byte LID_LENGTH_SHFT = (byte) 2;
    static final byte LEN_LENGTH_SHFT = (byte) 4;
    static final byte VER_LENGTH_SHFT = (byte) 6;
    static final byte LOG_ENTRY_TYP_SIZE = 1;
    static final byte LOG_ENTRY_NID_SIZE = 2;
    static final byte MAX_LOG_ENTRY_LID_SIZE = 6;
    static final byte MAX_LOG_ENTRY_CID_SIZE = LOG_ENTRY_NID_SIZE + MAX_LOG_ENTRY_LID_SIZE;
    static final byte MAX_LOG_ENTRY_LEN_SIZE = 3;
    static final byte MAX_LOG_ENTRY_VER_SIZE = 3;
    static final byte LOG_ENTRY_RID_SIZE = 1;
    static final byte LOG_ENTRY_SRC_SIZE = 2;
    static final byte LOG_ENTRY_EPO_SIZE = 2;
    static final byte INVALIDATION_MASK = (byte) 0x02;
    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractLogEntryHeader.class.getSimpleName());
    // Constants
    private static final byte PRIMLOG_TYPE_MASK = (byte) 0x03;
    private static final byte SECLOG_TYPE_MASK = (byte) 0x01;
    private static final Checksum CRC = new CRC32();
    private static final AbstractLogEntryHeader DEFAULT_PRIM_LOG_ENTRY_HEADER = new DefaultPrimLogEntryHeader();
    private static final AbstractLogEntryHeader MIGRATION_PRIM_LOG_ENTRY_HEADER = new MigrationPrimLogEntryHeader();
    private static final AbstractLogEntryHeader DEFAULT_SEC_LOG_ENTRY_HEADER = new DefaultSecLogEntryHeader();
    private static final AbstractLogEntryHeader MIGRATION_SEC_LOG_ENTRY_HEADER = new MigrationSecLogEntryHeader();
    // Attributes
    private static byte ms_logEntryCRCSize = (byte) 4;
    private static boolean ms_useChecksum = true;

    // Methods

    /**
     * Returns the maximum log entry header size
     *
     * @return the size
     */
    public abstract short getMaxHeaderSize();

    /**
     * Returns the offset for conversion
     *
     * @return the offset
     */
    public abstract short getConversionOffset();

    /**
     * Returns the checksum length
     *
     * @return the checksum length
     */
    static byte getCRCSize() {
        return ms_logEntryCRCSize;
    }

    /**
     * Sets the crc size
     *
     * @param p_useChecksum
     *     whether a checksum is added or not
     * @note Must be called before the first log entry header is created
     */
    public static void setCRCSize(final boolean p_useChecksum) {
        if (!p_useChecksum) {
            ms_logEntryCRCSize = (byte) 0;
        }
        ms_useChecksum = p_useChecksum;
    }

    /**
     * Returns the NodeID offset
     *
     * @return the offset
     */
    protected abstract short getNIDOffset();

    /**
     * Returns the LocalID offset
     *
     * @return the offset
     */
    protected abstract short getLIDOffset();

    /**
     * Adds checksum to entry header
     *
     * @param p_buffer
     *     the byte array
     * @param p_offset
     *     the offset within buffer
     * @param p_size
     *     the size of the complete log entry
     * @param p_logEntryHeader
     *     the LogEntryHeader
     * @param p_bytesUntilEnd
     *     number of bytes until wrap around
     */
    public static void addChecksum(final byte[] p_buffer, final int p_offset, final int p_size, final AbstractLogEntryHeader p_logEntryHeader,
        final int p_bytesUntilEnd) {
        final short headerSize = p_logEntryHeader.getHeaderSize(p_buffer, p_offset);
        final short crcOffset = p_logEntryHeader.getCRCOffset(p_buffer, p_offset);
        int checksum;

        CRC.reset();
        if (p_size <= p_bytesUntilEnd) {
            CRC.update(p_buffer, p_offset + headerSize, p_size - headerSize);
            checksum = (int) CRC.getValue();

            for (int i = 0; i < ms_logEntryCRCSize; i++) {
                p_buffer[p_offset + crcOffset + i] = (byte) (checksum >> i * 8 & 0xff);
            }
        } else {
            if (p_bytesUntilEnd < headerSize) {
                CRC.update(p_buffer, headerSize - p_bytesUntilEnd, p_size - headerSize);
                checksum = (int) CRC.getValue();

                if (p_bytesUntilEnd <= crcOffset) {
                    for (int i = 0; i < ms_logEntryCRCSize; i++) {
                        p_buffer[crcOffset - p_bytesUntilEnd + i] = (byte) (checksum >> i * 8 & 0xff);
                    }
                } else {
                    for (int i = 0; i < ms_logEntryCRCSize; i++) {
                        if (p_bytesUntilEnd - crcOffset - i > 0) {
                            p_buffer[p_offset + crcOffset + i] = (byte) (checksum >> i * 8 & 0xff);
                        } else {
                            p_buffer[i - (p_bytesUntilEnd - crcOffset)] = (byte) (checksum >> i * 8 & 0xff);
                        }
                    }
                }
            } else if (p_bytesUntilEnd > headerSize) {
                CRC.update(p_buffer, p_offset + headerSize, p_bytesUntilEnd - headerSize);
                CRC.update(p_buffer, 0, p_size - headerSize - (p_bytesUntilEnd - headerSize));
                checksum = (int) CRC.getValue();

                for (int i = 0; i < ms_logEntryCRCSize; i++) {
                    p_buffer[p_offset + crcOffset + i] = (byte) (checksum >> i * 8 & 0xff);
                }
            } else {
                CRC.update(p_buffer, 0, p_size - headerSize);
                checksum = (int) CRC.getValue();

                for (int i = 0; i < ms_logEntryCRCSize; i++) {
                    p_buffer[p_offset + crcOffset + i] = (byte) (checksum >> i * 8 & 0xff);
                }
            }
        }

    }

    /**
     * Returns the corresponding AbstractLogEntryHeader of a primary log entry
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the AbstractLogEntryHeader
     */
    public static AbstractLogEntryHeader getPrimaryHeader(final byte[] p_buffer, final int p_offset) {
        AbstractLogEntryHeader ret = null;
        byte type;

        type = (byte) (p_buffer[p_offset] & PRIMLOG_TYPE_MASK);
        if (type == 0) {
            ret = DEFAULT_PRIM_LOG_ENTRY_HEADER;
        } else if (type == 1) {
            ret = MIGRATION_PRIM_LOG_ENTRY_HEADER;
        } else {
            // #if LOGGER >= ERROR
            LOGGER.error("Type of log entry header unknown!");
            // #endif /* LOGGER >= ERROR */
        }

        return ret;
    }

    /**
     * Returns the corresponding AbstractLogEntryHeader of a secondary log entry
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @param p_storesMigrations
     *     whether the secondary log this entry is in stores migrations or not
     * @return the AbstractLogEntryHeader
     */
    public static AbstractLogEntryHeader getSecondaryHeader(final byte[] p_buffer, final int p_offset, final boolean p_storesMigrations) {
        AbstractLogEntryHeader ret = null;
        byte type;

        type = (byte) (p_buffer[p_offset] & SECLOG_TYPE_MASK);
        if (type == 0) {
            if (!p_storesMigrations) {
                ret = DEFAULT_SEC_LOG_ENTRY_HEADER;
            } else {
                ret = MIGRATION_SEC_LOG_ENTRY_HEADER;
            }
        }

        return ret;
    }

    /**
     * Converts a log entry header from PrimaryWriteBuffer/Primary Log to a secondary log entry header
     * and copies the payload
     *
     * @param p_input
     *     the input buffer
     * @param p_inputOffset
     *     the input buffer offset
     * @param p_output
     *     the output buffer
     * @param p_outputOffset
     *     the output buffer offset
     * @param p_logEntrySize
     *     the length of the log entry
     * @param p_bytesUntilEnd
     *     the number of bytes to the end of the input buffer
     * @param p_conversionOffset
     *     the conversion offset
     * @return the number of written bytes
     */
    public static int convertAndPut(final byte[] p_input, final int p_inputOffset, final byte[] p_output, final int p_outputOffset, final int p_logEntrySize,
        final int p_bytesUntilEnd, final short p_conversionOffset) {
        int ret;

        // Set type field
        p_output[p_outputOffset] = p_input[p_inputOffset];
        if (p_logEntrySize <= p_bytesUntilEnd) {
            // Copy shortened header and payload
            System.arraycopy(p_input, p_inputOffset + p_conversionOffset, p_output, p_outputOffset + 1, p_logEntrySize - p_conversionOffset);
        } else {
            // Entry is bisected
            if (p_conversionOffset >= p_bytesUntilEnd) {
                // Copy shortened header and payload
                System.arraycopy(p_input, p_conversionOffset - p_bytesUntilEnd, p_output, p_outputOffset + 1, p_logEntrySize - p_conversionOffset);
            } else {
                // Copy shortened header and payload in two steps
                System.arraycopy(p_input, p_inputOffset + p_conversionOffset, p_output, p_outputOffset + 1, p_bytesUntilEnd - p_conversionOffset);
                System.arraycopy(p_input, 0, p_output, p_outputOffset + p_bytesUntilEnd - p_conversionOffset + 1,
                    p_logEntrySize - (p_bytesUntilEnd - p_conversionOffset));
            }
        }
        ret = p_logEntrySize - (p_conversionOffset - 1);

        return ret;
    }

    /**
     * Calculates the CRC32 checksum of a log entry's payload
     *
     * @param p_payload
     *     the payload
     * @return the checksum
     */
    public static int calculateChecksumOfPayload(final byte[] p_payload, final int p_offset, final int p_length) {

        CRC.reset();
        CRC.update(p_payload, p_offset, p_length);

        return (int) CRC.getValue();
    }

    /**
     * Returns the approximated log entry header size for secondary log (the version size can only be determined on
     * backup -> 1 byte as average value)
     *
     * @param p_logStoresMigrations
     *     whether the entry is in a secondary log for migrations or not
     * @param p_localID
     *     the LocalID
     * @param p_size
     *     the size
     * @return the maximum log entry header size for secondary log
     */
    public static short getApproxSecLogHeaderSize(final boolean p_logStoresMigrations, final long p_localID, final int p_size) {
        // Sizes for type, LocalID, length, epoch and checksum is precise, 1 byte for version is an approximation
        // because the
        // actual version is determined during logging on backup peer (at creation time it's size is 0 but it might be
        // bigger at some point)
        short ret =
            (short) (LOG_ENTRY_TYP_SIZE + getSizeForLocalIDField(p_localID) + getSizeForLengthField(p_size) + LOG_ENTRY_EPO_SIZE + 1 + ms_logEntryCRCSize);

        if (p_logStoresMigrations) {
            ret += LOG_ENTRY_NID_SIZE;
        }

        return ret;
    }

    /**
     * Returns the approximated log entry header size for secondary log (the version size can only be determined on
     * backup -> 1 byte as average value, without a localID the length can only be approximated -> 4 byte)
     *
     * @param p_logStoresMigrations
     *     whether the entry is in a secondary log for migrations or not
     * @param p_size
     *     the size
     * @return the maximum log entry header size for secondary log
     */
    public static short getApproxSecLogHeaderSize(final boolean p_logStoresMigrations, final int p_size) {
        // Sizes for type, LocalID, length, epoch and checksum is precise, 1 byte for version is an approximation
        // because the
        // actual version is determined during logging on backup peer (at creation time it's size is 0 but it might be
        // bigger at some point)
        short ret = (short) (LOG_ENTRY_TYP_SIZE + 4 + getSizeForLengthField(p_size) + LOG_ENTRY_EPO_SIZE + 1 + ms_logEntryCRCSize);

        if (p_logStoresMigrations) {
            ret += LOG_ENTRY_NID_SIZE;
        }

        return ret;
    }

    /**
     * Returns the maximum log entry header size for secondary log
     *
     * @param p_logStoresMigrations
     *     whether the entry is in a secondary log for migrations or not
     * @return the maximum log entry header size for secondary log
     */
    public static short getMaxSecLogHeaderSize(final boolean p_logStoresMigrations) {
        short ret;

        if (p_logStoresMigrations) {
            ret = MIGRATION_SEC_LOG_ENTRY_HEADER.getMaxHeaderSize();
        } else {
            ret = DEFAULT_SEC_LOG_ENTRY_HEADER.getMaxHeaderSize();
        }

        return ret;
    }

    /**
     * Flip eon in log entry header
     *
     * @param p_logEntryHeader
     *     the AbstractLogEntryHeader
     * @param p_buffer
     *     the buffer the log entry header is in
     * @param p_offset
     *     the offset within the buffer
     */
    public static void flipEon(final AbstractLogEntryHeader p_logEntryHeader, final byte[] p_buffer, final int p_offset) {
        final int offset = p_offset + p_logEntryHeader.getVEROffset(p_buffer, p_offset);

        p_buffer[offset + 1] ^= 1 << 15;
    }

    /**
     * Returns whether there is a checksum in log entry header or not
     *
     * @return whether there is a checksum in log entry header or not
     */
    static boolean useChecksum() {
        return ms_useChecksum;
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
    static void putType(final byte[] p_logEntry, final byte p_type, final short p_offset) {
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
    static void putRangeID(final byte[] p_logEntry, final byte p_rangeID, final short p_offset) {
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
    static void putSource(final byte[] p_logEntry, final short p_source, final short p_offset) {
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
    static void putChunkID(final byte[] p_logEntry, final long p_chunkID, final byte p_localIDSize, final short p_offset) {
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
    static void putLength(final byte[] p_logEntry, final byte p_length, final short p_offset) {
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
    static void putLength(final byte[] p_logEntry, final short p_length, final short p_offset) {
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
    static void putLength(final byte[] p_logEntry, final int p_length, final short p_offset) {
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
    static void putEpoch(final byte[] p_logEntry, final short p_epoch, final short p_offset) {
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
    static void putVersion(final byte[] p_logEntry, final byte p_version, final short p_offset) {
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
    static void putVersion(final byte[] p_logEntry, final short p_version, final short p_offset) {
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
    static void putVersion(final byte[] p_logEntry, final int p_version, final short p_offset) {
        for (int i = 0; i < MAX_LOG_ENTRY_VER_SIZE; i++) {
            p_logEntry[p_offset + i] = (byte) (p_version >> i * 8 & 0xff);
        }
    }

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
     * @return the number of bytes necessary to store given value
     */
    static byte generateTypeField(final byte p_type, final byte p_localIDSize, final byte p_lengthSize, final byte p_versionSize) {
        byte ret = p_type;

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
     * Puts checksum of log entry in log entry header
     *
     * @param p_logEntry
     *     log entry
     * @param p_checksum
     *     the checksum
     * @param p_offset
     *     the type-specific offset
     */
    protected static void putChecksum(final byte[] p_logEntry, final int p_checksum, final short p_offset) {
        for (int i = 0; i < ms_logEntryCRCSize; i++) {
            p_logEntry[p_offset + i] = (byte) (p_checksum >> i * 8 & 0xff);
        }
    }

    /**
     * Generates a log entry with filled-in header but without any payload
     *
     * @param p_chunkID
     *     the ChunkID
     * @param p_size
     *     the payload length
     * @param p_version
     *     the version
     * @param p_rangeID
     *     the RangeID
     * @param p_source
     *     the source NodeID
     * @return the log entry
     */
    public abstract byte[] createLogEntryHeader(long p_chunkID, int p_size, Version p_version, byte p_rangeID, short p_source);

    /**
     * Returns the type of a log entry
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the type
     */
    public abstract short getType(byte[] p_buffer, int p_offset);

    /**
     * Returns RangeID of a log entry
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the version
     */
    public abstract byte getRangeID(byte[] p_buffer, int p_offset);

    /**
     * Returns source of a log entry
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the NodeID
     */
    public abstract short getSource(byte[] p_buffer, int p_offset);

    /**
     * Returns NodeID of a log entry
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the NodeID
     */
    public abstract short getNodeID(byte[] p_buffer, int p_offset);

    /**
     * Returns the ChunkID or the LocalID of a log entry
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the ChunkID if secondary log stores migrations, the LocalID otherwise
     */
    public abstract long getCID(byte[] p_buffer, int p_offset);

    /**
     * Returns length of a log entry
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the length
     */
    public abstract int getLength(byte[] p_buffer, int p_offset);

    /**
     * Returns epoch and version of a log entry
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the epoch and version
     */
    public abstract Version getVersion(byte[] p_buffer, int p_offset);

    /**
     * Returns the checksum of a log entry's payload
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the checksum
     */
    public abstract int getChecksum(byte[] p_buffer, int p_offset);

    /**
     * Checks whether this log entry was migrated or not
     *
     * @return whether this log entry was migrated or not
     */
    public abstract boolean wasMigrated();

    /**
     * Returns the log entry header size
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the size
     */
    public abstract short getHeaderSize(byte[] p_buffer, int p_offset);

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
    public abstract boolean readable(byte[] p_buffer, int p_offset, int p_bytesUntilEnd);

    /**
     * Prints the log header
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     */
    public abstract void print(byte[] p_buffer, int p_offset);

    /**
     * Returns the length offset
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the offset
     */
    protected abstract short getLENOffset(byte[] p_buffer, int p_offset);

    /**
     * Returns the version offset
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the offset
     */
    protected abstract short getVEROffset(byte[] p_buffer, int p_offset);

    /**
     * Returns the checksum offset
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the offset
     */
    protected abstract short getCRCOffset(byte[] p_buffer, int p_offset);

}
