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

package de.hhu.bsinfo.dxram.data;

import java.nio.ByteBuffer;

/**
 * Utils class with helpers for sending chunk related messages.
 * This helps making use of the additional status byte provided with every message.
 * Status byte as bit field:
 * 0: length field flag, 0 = length field = number of chunks in message, 1 = length field indicates size of additional
 * length field in message
 * 1 - 5: either number of chunks in message or length field size in bytes
 * 6: lock acquire flag
 * 7: 0 = read lock, 1 = write lock if lock acquire flag set
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 05.01.2016
 */
public final class ChunkMessagesMetadataUtils {

    private static final int BIT_OFFSET_FLAG_LENGTH_FIELD = 0;
    private static final int BIT_OFFSET_LENGTH_FIELD = 1;
    private static final int BIT_OFFSET_FLAG_LOCK_ACQUIRE = 6;
    private static final int BIT_OFFSET_FLAG_LOCK_TYPE = 7;

    private static final int BIT_MASK_LENGTH_AREA = 0x3F;
    private static final int BIT_MASK_LENGTH_FIELD = 0x1F;

    private static final int BIT_MASK_LOCK_AREA = 0x03;

    /**
     * Static class.
     */
    private ChunkMessagesMetadataUtils() {

    }

    /**
     * Set the number of items to send with the message in the status code provided.
     * This will not modify any other existing flags.
     *
     * @param p_statusCode
     *     Status code to set the number of items for.
     * @param p_numItems
     *     Number of items to send and indicate in the status byte.
     * @return New status byte with modified flags for the specified number of items.
     */
    public static byte setNumberOfItemsToSend(final byte p_statusCode, final int p_numItems) {
        byte v = p_statusCode;

        if (p_numItems <= BIT_MASK_LENGTH_FIELD) {
            v &= ~BIT_MASK_LENGTH_AREA;

            // don't need additional length field
            v &= ~(1 << BIT_OFFSET_FLAG_LENGTH_FIELD);
            v |= (p_numItems & BIT_MASK_LENGTH_FIELD) << BIT_OFFSET_LENGTH_FIELD;
        } else {
            v &= ~BIT_MASK_LENGTH_AREA;
            // indicate we use a length field
            v |= 1 << BIT_OFFSET_FLAG_LENGTH_FIELD;

            if (p_numItems <= 0xFF) {
                // 0 indicates length field 1 byte
            } else if (p_numItems <= 0xFFFF) {
                v |= 1 << 4;
            } else if (p_numItems <= 0xFFFFFF) {
                v |= 2 << 4;
            } else {
                v |= 3 << 4;
            }
        }

        return v;
    }

    /**
     * Get the size of the additional length field in the message payload from the status code.
     *
     * @param p_statusCode
     *     Status code to get the information from.
     * @return Size in bytes of the additional length field in the message payload or 0, if the total number of items is stored with the status code.
     */
    public static int getSizeOfAdditionalLengthField(final byte p_statusCode) {
        int size = 0;

        if ((p_statusCode & 1 << BIT_OFFSET_FLAG_LENGTH_FIELD) > 0) {
            size = (p_statusCode >> BIT_OFFSET_LENGTH_FIELD & BIT_MASK_LENGTH_FIELD) / 8;
            // 0 counts as 1 byte length field
            size++;
        }

        return size;
    }

    /**
     * Adds the additional length field to the payload buffer (if necessary). The buffer needs to be
     * at the correct position for the field to write.
     *
     * @param p_status
     *     Status byte of the message.
     * @param p_buffer
     *     Payload buffer.
     * @param p_numItems
     *     Number of items to send with this message.
     */
    public static void setNumberOfItemsInMessageBuffer(final byte p_status, final ByteBuffer p_buffer, final int p_numItems) {
        int sizeAdditionalLengthField = getSizeOfAdditionalLengthField(p_status);

        switch (sizeAdditionalLengthField) {
            case 0:
                // length already set with status byte
                break;
            case 1:
                p_buffer.put((byte) (p_numItems & 0xFF));
                break;
            case 2:
                p_buffer.putShort((short) (p_numItems & 0xFFFF));
                break;
            case 3:
                p_buffer.putShort((short) (p_numItems >> 8 & 0xFFFF));
                p_buffer.put((byte) (p_numItems & 0xFF));
                break;
            case 4:
                p_buffer.putInt(p_numItems);
                break;
            default:
                assert false;
                break;
        }
    }

    /**
     * Get the number of sent items from the message payload buffer. The buffer needs to be at the correct
     * position for the length field to be read correctly and will be advanced after reading.
     *
     * @param p_status
     *     Status byte of the message.
     * @param p_buffer
     *     Payload buffer.
     * @return Number of items in the message of the provided payload buffer, either taken from the status byte or the
     * buffer (depending on the status flags set).
     */
    public static int getNumberOfItemsFromMessageBuffer(final byte p_status, final ByteBuffer p_buffer) {
        int sizeAdditionalLengthField = getSizeOfAdditionalLengthField(p_status);
        int numChunks = 0;

        switch (sizeAdditionalLengthField) {
            case 0:
                numChunks = getNumberOfItemsSent(p_status);
                break;
            case 1:
                numChunks = p_buffer.get() & 0xFF;
                break;
            case 2:
                numChunks = p_buffer.getShort() & 0xFFFF;
                break;
            case 3:
                numChunks = (p_buffer.getShort() & 0xFFFF) << 8 | p_buffer.get() & 0xFF;
                break;
            case 4:
                numChunks = p_buffer.getInt();
                break;
            default:
                assert false;
                break;
        }

        return numChunks;
    }

    /**
     * Set the read lock flag within the status code. Other flags than the lock flags are not altered.
     *
     * @param p_statusCode
     *     Status code to modify.
     * @param p_set
     *     True to set the read lock flag, false to fully clear the lock acquire flag.
     * @return Modified status code.
     */
    public static byte setReadLockFlag(final byte p_statusCode, final boolean p_set) {
        byte v = p_statusCode;

        if (p_set) {
            v |= 1 << BIT_OFFSET_FLAG_LOCK_ACQUIRE;
            v &= ~(1 << BIT_OFFSET_FLAG_LOCK_TYPE);
        } else {
            v &= ~(1 << BIT_OFFSET_FLAG_LOCK_ACQUIRE);
        }

        return v;
    }

    /**
     * Set the write lock flag within the status code. Other flags than the lock flags are not altered.
     *
     * @param p_statusCode
     *     Status code to modify.
     * @param p_set
     *     True to set the write lock flag, false to fully clear the lock acquire flag.
     * @return Modified status code.
     */
    public static byte setWriteLockFlag(final byte p_statusCode, final boolean p_set) {
        byte v = p_statusCode;

        if (p_set) {
            v |= 1 << BIT_OFFSET_FLAG_LOCK_ACQUIRE;
            v |= 1 << BIT_OFFSET_FLAG_LOCK_TYPE;
        } else {
            v &= ~(1 << BIT_OFFSET_FLAG_LOCK_ACQUIRE);
        }

        return v;
    }

    /**
     * Check if the lock acquire flag is set within the status code.
     *
     * @param p_statusCode
     *     Status code to check.
     * @return True if lock acquire flag is set, false otherwise.
     */
    public static boolean isLockAcquireFlagSet(final byte p_statusCode) {
        return (p_statusCode & 1 << BIT_OFFSET_FLAG_LOCK_ACQUIRE) > 0;
    }

    /**
     * Check if the read lock flag is set within the status code.
     *
     * @param p_statusCode
     *     Status code to get the flags from.
     * @return True if read lock flag is set, false if either write lock set or lock acquire cleared.
     */
    public static boolean isReadLockFlagSet(final byte p_statusCode) {
        return (p_statusCode & 1 << BIT_OFFSET_FLAG_LOCK_ACQUIRE) > 0 && (p_statusCode & 1 << BIT_OFFSET_FLAG_LOCK_TYPE) == 0;
    }

    /**
     * Check if the write lock flag is set within the status code.
     *
     * @param p_statusCode
     *     Status code to get the flags from.
     * @return True if write lock flag is set, false if either read lock set or lock acquire cleared.
     */
    public static boolean isWriteLockFlagSet(final byte p_statusCode) {
        return (p_statusCode & 1 << BIT_OFFSET_FLAG_LOCK_ACQUIRE) > 0 && (p_statusCode & 1 << BIT_OFFSET_FLAG_LOCK_TYPE) > 0;
    }

    /**
     * Clear the lock acquire flag on the status code.
     *
     * @param p_statusCode
     *     Status code to modify.
     * @return Modified status code with lock flag cleared.
     */
    public static byte clearLockFlag(final byte p_statusCode) {
        return (byte) (p_statusCode & ~(BIT_MASK_LOCK_AREA << BIT_OFFSET_FLAG_LOCK_ACQUIRE));
    }

    /**
     * Extract the number of items sent from the provided status code.
     *
     * @param p_statusCode
     *     Status code from a message (see class header description for structure).
     * @return 0-32 items sent or -1 if the status code does not contain the number of items anymore (> 32).
     */
    private static int getNumberOfItemsSent(final byte p_statusCode) {
        int size;

        if ((p_statusCode & 1 << BIT_OFFSET_FLAG_LENGTH_FIELD) > 0) {
            size = -1;
        } else {
            size = p_statusCode >> BIT_OFFSET_LENGTH_FIELD & BIT_MASK_LENGTH_FIELD;
        }

        return size;
    }
}
