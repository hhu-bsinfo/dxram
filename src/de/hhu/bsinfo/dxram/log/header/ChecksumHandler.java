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

import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxutils.jni.JNINativeCRCGenerator;

/**
 * Helper class for generating 32-bit checksums.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 25.01.2017
 */
public final class ChecksumHandler {

    // Attributes
    private static byte ms_logEntryCRCSize = (byte) 4; // Do not change!
    private static boolean ms_useChecksum = true;
    private static boolean ms_native;

    /**
     * Hidden constructor
     */
    private ChecksumHandler() {
    }

    /**
     * Calculates the CRC32 checksum of a log entry's payload
     *
     * @param p_bufferWrapper
     *         the payload
     * @param p_offset
     *         the offset within b
     * @return the checksum
     */
    public static int calculateChecksumOfPayload(final DirectByteBufferWrapper p_bufferWrapper, final int p_offset, final int p_length) {
        if (ms_native) {
            return JNINativeCRCGenerator.hashNative(0, p_bufferWrapper.getAddress(), p_offset, p_length);
        } else {
            return JNINativeCRCGenerator.hashHeap(0, p_bufferWrapper.getBuffer().array(), p_offset, p_length);
        }
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
     *         whether a checksum is added or not
     * @note Must be called before the first log entry header is created
     */
    public static void setCRCSize(final boolean p_useChecksum) {
        if (!p_useChecksum) {
            ms_logEntryCRCSize = (byte) 0;
        }
        ms_useChecksum = p_useChecksum;
    }

    /**
     * Whether to use native ByteBuffers or heap ByteBuffers for reading/writing from/to files.
     *
     * @param p_useNativeBuffers
     *         whether to use native ByteBuffers (true) or heap ByteBuffers (false)
     */
    public static void useNativeBuffers(final boolean p_useNativeBuffers) {
        ms_native = p_useNativeBuffers;
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
     * @param p_bufferWrapper
     *         the byte array
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
    static void addChecksum(final DirectByteBufferWrapper p_bufferWrapper, final int p_offset, final int p_size,
            final AbstractPrimLogEntryHeader p_logEntryHeader, final int p_headerSize, final int p_bytesUntilEnd) {
        ByteBuffer buffer = p_bufferWrapper.getBuffer();
        final short crcOffset = p_logEntryHeader.getCRCOffset(buffer, p_offset);
        int checksum = 0;

        if (p_size + p_headerSize <= p_bytesUntilEnd) {
            if (ms_native) {
                checksum = JNINativeCRCGenerator.hashNative(checksum, p_bufferWrapper.getAddress(), p_offset + p_headerSize, p_size);
            } else {
                checksum = JNINativeCRCGenerator.hashHeap(checksum, p_bufferWrapper.getBuffer().array(), p_offset + p_headerSize, p_size);
            }

            buffer.putInt(p_offset + crcOffset, checksum);
        } else {
            if (p_bytesUntilEnd < p_headerSize) {
                if (ms_native) {
                    checksum = JNINativeCRCGenerator.hashNative(checksum, p_bufferWrapper.getAddress(), p_headerSize - p_bytesUntilEnd, p_size);
                } else {
                    checksum = JNINativeCRCGenerator.hashHeap(checksum, p_bufferWrapper.getBuffer().array(), p_headerSize - p_bytesUntilEnd, p_size);
                }

                if (p_bytesUntilEnd <= crcOffset) {
                    buffer.putInt(crcOffset - p_bytesUntilEnd, checksum);
                } else {
                    int i;
                    for (i = 0; i < p_bytesUntilEnd - crcOffset; i++) {
                        buffer.put(p_offset + crcOffset + i, (byte) (checksum >> i * 8 & 0xFF));
                    }
                    for (; i < ms_logEntryCRCSize; i++) {
                        buffer.put(i - (p_bytesUntilEnd - crcOffset), (byte) (checksum >> i * 8 & 0xFF));
                    }
                }
            } else if (p_bytesUntilEnd > p_headerSize) {
                if (ms_native) {
                    checksum =
                            JNINativeCRCGenerator.hashNative(checksum, p_bufferWrapper.getAddress(), p_offset + p_headerSize, p_bytesUntilEnd - p_headerSize);
                    checksum = JNINativeCRCGenerator.hashNative(checksum, p_bufferWrapper.getAddress(), 0, p_size - (p_bytesUntilEnd - p_headerSize));
                } else {
                    checksum = JNINativeCRCGenerator
                            .hashHeap(checksum, p_bufferWrapper.getBuffer().array(), p_offset + p_headerSize, p_bytesUntilEnd - p_headerSize);
                    checksum = JNINativeCRCGenerator.hashHeap(checksum, p_bufferWrapper.getBuffer().array(), 0, p_size - (p_bytesUntilEnd - p_headerSize));
                }

                buffer.putInt(p_offset + crcOffset, checksum);
            } else {
                if (ms_native) {
                    checksum = JNINativeCRCGenerator.hashNative(checksum, p_bufferWrapper.getAddress(), 0, p_size);
                } else {
                    checksum = JNINativeCRCGenerator.hashHeap(checksum, p_bufferWrapper.getBuffer().array(), 0, p_size);
                }

                buffer.putInt(p_offset + crcOffset, checksum);
            }
        }
    }

}
