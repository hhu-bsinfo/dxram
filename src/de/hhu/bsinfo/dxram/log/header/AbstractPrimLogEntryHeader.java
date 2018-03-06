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

import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxram.log.storage.Version;

/**
 * Extends AbstractLogEntryHeader for implementing access to primary log entry header.
 * Log entry headers are read and written with absolute methods (position is untouched), only!
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 25.01.2017
 */
public abstract class AbstractPrimLogEntryHeader extends AbstractLogEntryHeader {

    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractPrimLogEntryHeader.class.getSimpleName());
    private static final AbstractPrimLogEntryHeader PRIM_LOG_ENTRY_HEADER = new PrimLogEntryHeader();

    // Methods

    @Override
    abstract short getNIDOffset();

    @Override
    abstract short getLIDOffset();

    /**
     * Generates a log entry with filled-in header but without any payload
     *
     * @param p_chunkID
     *         the ChunkID
     * @param p_size
     *         the payload length
     * @param p_version
     *         the version
     * @param p_rangeID
     *         the RangeID
     * @param p_owner
     *         the owner NodeID
     * @param p_originalOwner
     *         the original owner (before recovery)
     * @param p_timestamp
     *         the timestamp or 0 if timestamps are disabled
     */
    public abstract ByteBuffer createLogEntryHeader(final long p_chunkID, final int p_size, final Version p_version, final short p_rangeID, final short p_owner,
            final short p_originalOwner, final int p_timestamp);

    /**
     * Returns RangeID of a log entry
     *
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the version
     */
    public abstract short getRangeID(final ByteBuffer p_buffer, final int p_offset);

    /**
     * Returns owner of a log entry
     *
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the NodeID
     */
    public abstract short getOwner(final ByteBuffer p_buffer, final int p_offset);

    /**
     * Prints the log header
     *
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     */
    public abstract void print(final ByteBuffer p_buffer, final int p_offset);

    /**
     * Returns the corresponding AbstractPrimLogEntryHeader
     *
     * @return the AbstractPrimLogEntryHeader
     */
    public static AbstractPrimLogEntryHeader getHeader() {
        return PRIM_LOG_ENTRY_HEADER;
    }

    /**
     * Adds chaining ID to log entry header
     *
     * @param p_buffer
     *         the byte array
     * @param p_offset
     *         the offset within buffer
     * @param p_chainingID
     *         the chaining ID
     * @param p_chainSize
     *         the number of segments in chain
     * @param p_logEntryHeader
     *         the LogEntryHeader
     */
    public static void addChainingIDAndChainSize(final ByteBuffer p_buffer, final int p_offset, final byte p_chainingID, final byte p_chainSize,
            final AbstractPrimLogEntryHeader p_logEntryHeader) {
        int offset = p_logEntryHeader.getCHAOffset(p_buffer, p_offset);

        p_buffer.put(offset, (byte) (p_chainingID & 0xFF));
        p_buffer.put(offset + 1, (byte) (p_chainSize & 0xFF));
    }

    /**
     * Adjusts the length in log entry header. Is used for chained log entries, only.
     *
     * @param p_buffer
     *         the byte array
     * @param p_offset
     *         the offset within buffer
     * @param p_newLength
     *         the new length
     * @param p_logEntryHeader
     *         the LogEntryHeader
     */
    public static void adjustLength(final ByteBuffer p_buffer, final int p_offset, final int p_newLength, final AbstractPrimLogEntryHeader p_logEntryHeader) {
        int offset = p_logEntryHeader.getLENOffset(p_buffer, p_offset);
        int lengthSize = p_logEntryHeader.getVEROffset(p_buffer, p_offset) - offset;

        for (int i = 0; i < lengthSize; i++) {
            p_buffer.put(offset + i, (byte) (p_newLength >> i * 8 & 0xFF));
        }
    }

    /**
     * Adds checksum to entry header
     *
     * @param p_bufferWrapper
     *         the byte buffer wrapper
     * @param p_offset
     *         the offset within buffer
     * @param p_size
     *         the size of payload
     * @param p_logEntryHeader
     *         the LogEntryHeader
     * @param p_headerSize
     *         the size of the header
     * @param p_bytesUntilEnd
     *         number of bytes until wrap around
     */
    public static void addChecksum(final DirectByteBufferWrapper p_bufferWrapper, final int p_offset, final int p_size,
            final AbstractPrimLogEntryHeader p_logEntryHeader, final int p_headerSize, final int p_bytesUntilEnd) {
        ChecksumHandler.addChecksum(p_bufferWrapper, p_offset, p_size, p_logEntryHeader, p_headerSize, p_bytesUntilEnd);
    }

    /**
     * Converts a log entry header from PrimaryWriteBuffer/Primary Log to a secondary log entry header
     * and copies the payload
     *
     * @param p_input
     *         the input buffer
     * @param p_inputOffset
     *         the input buffer offset
     * @param p_output
     *         the output buffer
     * @param p_logEntrySize
     *         the length of the log entry
     * @param p_bytesUntilEnd
     *         the number of bytes to the end of the input buffer
     * @param p_conversionOffset
     *         the conversion offset
     */
    public static void convertAndPut(final ByteBuffer p_input, final int p_inputOffset, final ByteBuffer p_output, final int p_logEntrySize,
            final int p_bytesUntilEnd, final short p_conversionOffset) {
        // Set type field
        p_output.put(p_input.get(p_inputOffset));
        if (p_logEntrySize <= p_bytesUntilEnd) {
            // Copy shortened header and payload
            p_input.position(p_inputOffset + p_conversionOffset);
            p_input.limit(p_inputOffset + p_logEntrySize);
            p_output.put(p_input);
        } else {
            // Entry is bisected
            if (p_conversionOffset >= p_bytesUntilEnd) {
                // Copy shortened header and payload
                p_input.position(p_conversionOffset - p_bytesUntilEnd);
                p_input.limit(p_logEntrySize - p_bytesUntilEnd);
                p_output.put(p_input);
            } else {
                // Copy shortened header and payload in two steps
                p_input.position(p_inputOffset + p_conversionOffset);
                p_output.put(p_input);

                p_input.position(0);
                p_input.limit(p_logEntrySize - p_bytesUntilEnd);
                p_output.put(p_input);
            }
        }
    }

    /**
     * Returns the offset for conversion
     *
     * @return the offset
     */
    public static short getConversionOffset(final ByteBuffer p_buffer, final int p_offset) {
        short ret;
        byte type;

        type = (byte) (p_buffer.get(p_offset) & TYPE_MASK);
        if (type == 0) {
            // Convert into DefaultSecLogEntryHeader by skipping NodeID
            ret = PRIM_LOG_ENTRY_HEADER.getLIDOffset();
        } else {
            // Convert into MigrationSecLogEntryHeader
            ret = PRIM_LOG_ENTRY_HEADER.getNIDOffset();
        }

        return ret;
    }

    @Override
    public long getCID(final ByteBuffer p_buffer, final int p_offset) {
        return ((long) getNodeID(p_buffer, p_offset) << 48) + getLID(p_buffer, p_offset);
    }

    @Override
    short getNodeID(final ByteBuffer p_buffer, final int p_offset) {
        final int offset = p_offset + getNIDOffset();

        return p_buffer.getShort(offset);
    }

}
