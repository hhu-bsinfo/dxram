/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

import de.hhu.bsinfo.dxutils.jni.JNINativeCRCGenerator;

/**
 * Helper class for generating 32-bit checksums.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 25.01.2017
 */
public final class ChecksumHandler {

    // Attributes
    private static byte ms_logEntryCRCSize = (byte) 4;
    private static boolean ms_useChecksum = true;

    /**
     * Hidden constructor
     */
    private ChecksumHandler() {
    }

    /**
     * Calculates the CRC32 checksum of a log entry's payload
     *
     * @param p_payload
     *     the payload
     * @param p_offset
     *     the offset within b
     * @return the checksum
     */
    public static int calculateChecksumOfPayload(final byte[] p_payload, final int p_offset, final int p_length) {
        return JNINativeCRCGenerator.hash(0, p_payload, p_offset, p_length);
    }

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
     * Returns whether there is a checksum in log entry header or not
     *
     * @return whether there is a checksum in log entry header or not
     */
    static boolean checksumsEnabled() {
        return ms_useChecksum;
    }

    /**
     * Adds checksum to entry header
     *
     * @param p_buffer
     *     the byte array
     * @param p_offset
     *     the offset within buffer
     * @param p_size
     *     the size of payload
     * @param p_logEntryHeader
     *     the LogEntryHeader
     * @param p_bytesUntilEnd
     *     number of bytes until wrap around
     */
    static void addChecksum(final byte[] p_buffer, final int p_offset, final int p_size, final AbstractPrimLogEntryHeader p_logEntryHeader,
        final int p_bytesUntilEnd) {
        final short headerSize = p_logEntryHeader.getHeaderSize(p_buffer, p_offset);
        final short crcOffset = p_logEntryHeader.getCRCOffset(p_buffer, p_offset);
        int checksum = 0;

        if (p_size + headerSize <= p_bytesUntilEnd) {
            checksum = JNINativeCRCGenerator.hash(checksum, p_buffer, p_offset + headerSize, p_size);

            for (int i = 0; i < ms_logEntryCRCSize; i++) {
                p_buffer[p_offset + crcOffset + i] = (byte) (checksum >> i * 8 & 0xff);
            }
        } else {
            if (p_bytesUntilEnd < headerSize) {
                checksum = JNINativeCRCGenerator.hash(checksum, p_buffer, headerSize - p_bytesUntilEnd, p_size);

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
                checksum = JNINativeCRCGenerator.hash(checksum, p_buffer, p_offset + headerSize, p_bytesUntilEnd - headerSize);
                checksum = JNINativeCRCGenerator.hash(checksum, p_buffer, 0, p_size - (p_bytesUntilEnd - headerSize));

                for (int i = 0; i < ms_logEntryCRCSize; i++) {
                    p_buffer[p_offset + crcOffset + i] = (byte) (checksum >> i * 8 & 0xff);
                }
            } else {
                checksum = JNINativeCRCGenerator.hash(checksum, p_buffer, 0, p_size);

                for (int i = 0; i < ms_logEntryCRCSize; i++) {
                    p_buffer[p_offset + crcOffset + i] = (byte) (checksum >> i * 8 & 0xff);
                }
            }
        }
    }

}
