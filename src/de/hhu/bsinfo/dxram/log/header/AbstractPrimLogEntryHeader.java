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

import de.hhu.bsinfo.dxram.log.storage.Version;

/**
 * Extends AbstractLogEntryHeader for implementing access to primary log entry header.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 25.01.2017
 */
public abstract class AbstractPrimLogEntryHeader extends AbstractLogEntryHeader {

    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractPrimLogEntryHeader.class.getSimpleName());
    private static final AbstractPrimLogEntryHeader DEFAULT_PRIM_LOG_ENTRY_HEADER = new DefaultPrimLogEntryHeader();
    private static final AbstractPrimLogEntryHeader MIGRATION_PRIM_LOG_ENTRY_HEADER = new MigrationPrimLogEntryHeader();

    // Methods

    /**
     * Returns the offset for conversion
     *
     * @return the offset
     */
    public abstract short getConversionOffset();

    @Override
    abstract short getNIDOffset();

    @Override
    abstract short getLIDOffset();

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
    public abstract byte[] createLogEntryHeader(final long p_chunkID, final int p_size, final Version p_version, final byte p_rangeID, final short p_source);

    /**
     * Returns RangeID of a log entry
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the version
     */
    public abstract byte getRangeID(final byte[] p_buffer, final int p_offset);

    /**
     * Returns source of a log entry
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the NodeID
     */
    public abstract short getSource(final byte[] p_buffer, final int p_offset);

    /**
     * Checks whether this log entry was migrated or not
     *
     * @return whether this log entry was migrated or not
     */
    public abstract boolean wasMigrated();

    /**
     * Prints the log header
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     */
    public abstract void print(final byte[] p_buffer, final int p_offset);

    /**
     * Returns the corresponding AbstractPrimLogEntryHeader
     *
     * @param p_buffer
     *     buffer with log entries
     * @param p_offset
     *     offset in buffer
     * @return the AbstractPrimLogEntryHeader
     */
    public static AbstractPrimLogEntryHeader getHeader(final byte[] p_buffer, final int p_offset) {
        AbstractPrimLogEntryHeader ret = null;
        byte type;

        type = (byte) (p_buffer[p_offset] & TYPE_MASK);
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
     * Adds chaining ID to log entry header
     *
     * @param p_buffer
     *     the byte array
     * @param p_offset
     *     the offset within buffer
     * @param p_chainingID
     *     the version
     * @param p_logEntryHeader
     *     the LogEntryHeader
     * @param p_bytesUntilEnd
     *     number of bytes until wrap around
     */
    public static void addChainingID(final byte[] p_buffer, final int p_offset, final byte p_chainingID, final AbstractPrimLogEntryHeader p_logEntryHeader,
        final int p_bytesUntilEnd) {
        int offset = p_logEntryHeader.getCHAOffset(p_buffer, p_offset);

        if (offset < p_bytesUntilEnd) {
            offset += p_offset;
        } else {
            offset -= p_bytesUntilEnd;
        }

        p_buffer[offset] = (byte) (p_chainingID & 0xff);
    }

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
    public static void addChecksum(final byte[] p_buffer, final int p_offset, final int p_size, final AbstractPrimLogEntryHeader p_logEntryHeader,
        final int p_bytesUntilEnd) {
        ChecksumHandler.addChecksum(p_buffer, p_offset, p_size, p_logEntryHeader, p_bytesUntilEnd);
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

    @Override
    public long getCID(final byte[] p_buffer, final int p_offset) {
        return ((long) getNodeID(p_buffer, p_offset) << 48) + getLID(p_buffer, p_offset);
    }

    @Override
    short getNodeID(final byte[] p_buffer, final int p_offset) {
        final int offset = p_offset + getNIDOffset();

        return (short) ((p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8));
    }

}
