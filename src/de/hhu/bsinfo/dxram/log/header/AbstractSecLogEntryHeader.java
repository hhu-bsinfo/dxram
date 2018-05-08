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

/**
 * Extends AbstractLogEntryHeader for implementing access to secondary log entry header.
 * Log entry headers are read and written with absolute methods (position is untouched), only!
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 25.01.2017
 */
public abstract class AbstractSecLogEntryHeader extends AbstractLogEntryHeader {

    private static final AbstractSecLogEntryHeader DEFAULT_SEC_LOG_ENTRY_HEADER = new DefaultSecLogEntryHeader();
    private static final AbstractSecLogEntryHeader MIGRATION_SEC_LOG_ENTRY_HEADER = new MigrationSecLogEntryHeader();

    // Methods

    @Override
    abstract short getNIDOffset();

    @Override
    abstract short getLIDOffset();

    @Override
    abstract short getNodeID(final ByteBuffer p_buffer, final int p_offset);

    @Override
    public abstract long getCID(final ByteBuffer p_buffer, final int p_offset);

    public abstract boolean isMigrated();

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
     * Returns the corresponding AbstractSecLogEntryHeader
     *
     * @param p_buffer
     *         buffer with log entries
     * @param p_offset
     *         offset in buffer
     * @return the AbstractSecLogEntryHeader
     */
    public static AbstractSecLogEntryHeader getHeader(final ByteBuffer p_buffer, final int p_offset) {
        AbstractSecLogEntryHeader ret;
        byte type;

        type = p_buffer.get(p_offset);

        assert type != 0;

        type &= TYPE_MASK;
        if (type == 0) {
            ret = DEFAULT_SEC_LOG_ENTRY_HEADER;
        } else {
            ret = MIGRATION_SEC_LOG_ENTRY_HEADER;
        }

        return ret;
    }

    /**
     * Determines the maximum number of versions per backup range for a given default chunk size
     *
     * @param p_maxBackupRangeSize
     *         the maximum backup range size
     * @param p_defaultChunkSize
     *         the default chunk size
     * @param p_logStoresMigrations
     *         whether the calculation should consider migrations or not
     * @return the maximum number of versions
     */
    public static int getMaximumNumberOfVersions(final long p_maxBackupRangeSize, final int p_defaultChunkSize,
            final boolean p_logStoresMigrations) {
        if (!p_logStoresMigrations) {
            return (int) (p_maxBackupRangeSize / (p_defaultChunkSize +
                    AbstractSecLogEntryHeader.getApproxSecLogHeaderSize(false, 0, p_defaultChunkSize)));
        } else {
            return (int) (p_maxBackupRangeSize / (p_defaultChunkSize +
                    AbstractSecLogEntryHeader.getApproxSecLogHeaderSize(true, p_defaultChunkSize)));
        }
    }

    /**
     * Returns the approximated log entry header size for secondary log (the version size can only be determined on
     * backup -> 1 byte as average value)
     *
     * @param p_logStoresMigrations
     *         whether the entry is in a secondary log for migrations or not
     * @param p_localID
     *         the LocalID
     * @param p_size
     *         the size
     * @return the maximum log entry header size for secondary log
     */
    public static short getApproxSecLogHeaderSize(final boolean p_logStoresMigrations, final long p_localID,
            final int p_size) {
        // Sizes for type, LocalID, length, epoch and checksum is precise, 1 byte for version (and chaining)
        // is an approximation because the actual version is determined during logging on backup peer (at
        // creation time it's size is 0 but it might be bigger at some point)
        short ret = (short) (LOG_ENTRY_TYP_SIZE + getSizeForLocalIDField(p_localID) + getSizeForLengthField(p_size) +
                ms_timestampSize + LOG_ENTRY_EPO_SIZE + 1 + 1 + ChecksumHandler.getCRCSize());

        if (p_logStoresMigrations) {
            ret += LOG_ENTRY_NID_SIZE;
        }

        // Account additional log entry headers for chained log entries
        int parts = (int) Math.ceil((double) p_size / getMaxLogEntrySize());
        if (parts > 1) {
            ret = (short) ((ret + LOG_ENTRY_CHA_SIZE) * parts);
        }

        return ret;
    }

    /**
     * Returns the approximated log entry header size for secondary log (the version size can only be determined on
     * backup -> 1 byte as average value, without a localID the length can only be approximated -> 4 byte)
     *
     * @param p_logStoresMigrations
     *         whether the entry is in a secondary log for migrations or not
     * @param p_size
     *         the size
     * @return the maximum log entry header size for secondary log
     */
    public static short getApproxSecLogHeaderSize(final boolean p_logStoresMigrations, final int p_size) {
        // Sizes for type, length, epoch and checksum is precise, 2 bytes for LocalID, 1 byte for version (and chaining)
        // is an approximation because the actual version is determined during logging on backup peer (at creation time
        // it's size is 0 but it might be bigger at some point)
        short ret = (short) (LOG_ENTRY_TYP_SIZE + 4 + getSizeForLengthField(p_size) + ms_timestampSize +
                LOG_ENTRY_EPO_SIZE + 1 + 1 + ChecksumHandler.getCRCSize());

        if (p_logStoresMigrations) {
            ret += LOG_ENTRY_NID_SIZE;
        }

        // Account additional log entry headers for chained log entries
        int parts = (int) Math.ceil((float) p_size / getMaxLogEntrySize());
        if (parts > 1) {
            ret = (short) ((ret + 1) * parts);
        }

        return ret;
    }

    /**
     * Flip eon in log entry header
     *
     * @param p_buffer
     *         the buffer the log entry header is in
     * @param p_offset
     *         the offset within the buffer
     */
    public void flipEon(final ByteBuffer p_buffer, final int p_offset) {
        final int offset = p_offset + getVEROffset(p_buffer, p_offset);

        byte cur = p_buffer.get(offset + 1);
        p_buffer.put(offset + 1, (byte) (cur ^ 1 << 15));
        //p_buffer[offset + 1] ^= 1 << 15;
    }

}
