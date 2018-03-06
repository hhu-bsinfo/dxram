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

package de.hhu.bsinfo.soh;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.RandomAccessFileImExporter;

/**
 * Very efficient memory allocator for many small objects
 *
 * @author Florian Klein, florian.klein@hhu.de, 13.02.2014
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.11.2015
 */
public final class SmallObjectHeap implements Importable, Exportable {
    public static final int INVALID_ADDRESS = 0;
    static final byte POINTER_SIZE = 5;
    static final int SIZE_MARKER_BYTE = 1;
    static final byte ALLOC_BLOCK_FLAGS_OFFSET = 0x5;
    private static final Logger LOGGER = LogManager.getFormatterLogger(SmallObjectHeap.class.getSimpleName());
    private static final long MAX_SET_SIZE = (long) Math.pow(2, 30);
    private static final byte SMALL_BLOCK_SIZE = 64;
    private static final byte SINGLE_BYTE_MARKER = 0xF;
    // Attributes, have them accessible by the package to enable walking and analyzing the heap
    // don't modify or access them otherwise
    long m_baseFreeBlockList;
    int m_freeBlocksListSize = -1;
    long[] m_freeBlockListSizes;
    Storage m_memory;
    private int m_maxBlockSize;
    private int m_freeBlocksListCount = -1;
    private Status m_status;

    /**
     * Creates an instance of the object heap
     *
     * @param p_memory
     *         The underlying storage to use for this memory.
     * @param p_size
     *         The size of the memory in bytes.
     */
    public SmallObjectHeap(final Storage p_memory, final long p_size, final int p_maxBlockSize) {
        m_memory = p_memory;
        m_status = new Status();
        m_status.m_size = p_size;
        m_status.m_maxBlockSize = p_maxBlockSize;

        // #if LOGGER >= INFO
        LOGGER.info("Creating SmallObjectHeap, size %d bytes, max block size %d bytes", p_size, p_maxBlockSize);
        // #endif /* LOGGER >= INFO */

        m_memory.allocate(p_size);

        // Reset the memory block to zero. Do it in rather small sets to avoid ZooKeeper time-out
        int sets = (int) (p_size / MAX_SET_SIZE);
        for (int i = 0; i < sets; i++) {
            m_memory.set(MAX_SET_SIZE * i, MAX_SET_SIZE, (byte) 0);
        }
        if (p_size % MAX_SET_SIZE != 0) {
            m_memory.set(MAX_SET_SIZE * sets, (int) (p_size - sets * MAX_SET_SIZE), (byte) 0);
        }

        // according to memory size, have a proper amount of
        // free memory block lists
        // -2, because we don't need a free block list for the full memory
        // and the first size greater than the full memory size
        // detect highest bit using log2 to have proper memory sizes
        m_freeBlocksListCount = (int) (Math.log(p_size) / Math.log(2)) - 2;
        m_freeBlocksListSize = m_freeBlocksListCount * POINTER_SIZE;
        m_baseFreeBlockList = m_status.m_size - m_freeBlocksListSize;

        // Initializes the list sizes
        m_freeBlockListSizes = new long[m_freeBlocksListCount];

        m_freeBlockListSizes[0] = 12;
        m_freeBlockListSizes[1] = 24;
        m_freeBlockListSizes[2] = 36;
        m_freeBlockListSizes[3] = 48;

        for (int i = 4; i < m_freeBlocksListCount; i++) {
            // 64, 128, ...
            m_freeBlockListSizes[i] = (long) Math.pow(2, i + 2);
        }

        // #if LOGGER >= DEBUG
        LOGGER.debug("Created free block lists, m_freeBlocksListCount %d, m_freeBlocksListSize %d, m_baseFreeBlockList %d", m_freeBlocksListCount,
                m_freeBlocksListSize, m_baseFreeBlockList);
        // #endif /* LOGGER >= DEBUG */

        // Create one big free block
        // -2 for the marker bytes
        m_status.m_free = m_status.m_size - m_freeBlocksListSize - SIZE_MARKER_BYTE * 2;
        createFreeBlock(SIZE_MARKER_BYTE, m_status.m_free);
        m_status.m_freeBlocks = 1;
        m_status.m_freeSmall64ByteBlocks = 0;
    }

    public SmallObjectHeap(final String p_memDumpFile, final Storage p_memory) {
        m_memory = p_memory;

        File file = new File(p_memDumpFile);

        if (!file.exists()) {
            throw new MemoryRuntimeException("Cannot create heap from mem dump " + p_memDumpFile + ": file does not exist");
        }

        RandomAccessFileImExporter importer;
        try {
            importer = new RandomAccessFileImExporter(file);
        } catch (final FileNotFoundException e) {
            // cannot happen
            throw new MemoryRuntimeException("Illegal state", e);
        }

        importer.importObject(this);
    }

    /**
     * Extract the size of the length field of the allocated or free area
     * from the marker byte.
     *
     * @param p_marker
     *         Marker byte.
     * @return Size of the length field of block with specified marker byte.
     */
    private static int getSizeFromMarker(final int p_marker) {
        int ret;

        if (p_marker <= ALLOC_BLOCK_FLAGS_OFFSET) {
            // free block size
            ret = p_marker;
        } else {
            // allocated block sizes 1, 2, 3, 4 are used
            ret = p_marker - ALLOC_BLOCK_FLAGS_OFFSET;
        }

        return ret;
    }

    /**
     * Calculate the size of the length field for a given memory block size
     *
     * @param p_size
     *         Memory block size
     * @return Size of the length field to fit the size of the memory block
     */
    private static int calculateLengthFieldSizeAllocBlock(final int p_size) {
        if (p_size >= 1 << 24) {
            return 4;
        } else if (p_size >= 1 << 16) {
            return 3;
        } else if (p_size >= 1 << 8) {
            return 2;
        } else {
            return 1;
        }
    }

    /**
     * Check the memory bounds of an allocated memory region on access
     *
     * @param p_address
     *         Start address of the allocated memory block
     * @param p_lengthFieldSize
     *         Size of the length field
     * @param p_size
     *         Size of the memory block
     * @param p_offset
     *         Offset to access the black at
     * @param p_length
     *         Range of the access starting at offset
     * @return Dummy return for assert
     */
    private static boolean assertMemoryBlockBounds(final long p_address, final long p_lengthFieldSize, final long p_size, final long p_offset,
            final long p_length) {
        if (!(p_offset < p_size) || !(p_size >= p_length && p_offset + p_length <= p_size)) {
            throw new MemoryRuntimeException(
                    "Access at invalid offset " + p_offset + ", on address " + p_address + ", with length " + p_length + " for memory block size " + p_size +
                            ", size length field " + p_lengthFieldSize);
        }

        return true;
    }

    /**
     * Gets the status
     *
     * @return the status
     */
    public Status getStatus() {
        return m_status;
    }

    /**
     * Free all memory of the storage instance
     */
    public void destroy() {
        m_memory.free();
        m_memory = null;
    }

    /**
     * Full memory dump
     *
     * @param p_file
     *         Destination for the memory file to dump to
     */
    public void dump(final String p_file) {
        File file = new File(p_file);

        if (file.exists()) {
            if (!file.delete()) {
                throw new MemoryRuntimeException("Deleting existing file for memory dump failed");
            }
        } else {
            try {
                if (!file.createNewFile()) {
                    throw new MemoryRuntimeException("Creating file for memory dump failed");
                }
            } catch (final IOException e) {
                throw new MemoryRuntimeException("Creating file for memory dump failed", e);
            }
        }

        RandomAccessFileImExporter exporter;
        try {
            exporter = new RandomAccessFileImExporter(file);
        } catch (final FileNotFoundException e) {
            // not possible
            throw new MemoryRuntimeException("Illegal state", e);
        }

        exporter.exportObject(this);
        exporter.close();
    }

    /**
     * Allocate a memory block
     *
     * @param p_size
     *         the size of the block in bytes.
     * @return the address of the block or 0 if no free blocks available for the specified size
     */
    public long malloc(final int p_size) {
        return reserveBlock(p_size);
    }

    /**
     * Allocate multiple blocks in a single call. This falls back to normal malloc if the
     * allocator cannot find a single free block that fits all the sizes
     *
     * @param p_sizes
     *         Sizes for the blocks to allocate
     * @return List of addresses for the sizes on success, null on failure
     */
    public long[] multiMallocSizes(final int... p_sizes) {
        return multiMallocSizesUsedEntries(p_sizes.length, p_sizes);
    }

    /**
     * Allocate multiple blocks in a single call. This falls back to normal malloc if the
     * allocator cannot find a single free block that fits all the sizes
     *
     * @param p_sizes
     *         Sizes for the blocks to allocate
     * @param p_usedEntries
     *         First n elements to be used of size array
     * @return List of addresses for the sizes on success, null on failure
     */
    public long[] multiMallocSizesUsedEntries(final int p_usedEntries, final int... p_sizes) {
        long[] ret;

        // number of marker bytes to separate blocks
        // -1: one marker byte is already part of the free block
        int bigChunkSize = p_usedEntries - 1;

        for (int i = 0; i < p_usedEntries; i++) {
            if (p_sizes[i] > m_status.m_maxBlockSize) {
                throw new MemoryRuntimeException("Req allocation size " + p_sizes[i] + " is exceeding max memory block size " + m_status.m_maxBlockSize);
            }

            if (p_sizes[i] <= 0) {
                throw new MemoryRuntimeException("Invalid size " + p_sizes[i]);
            }

            bigChunkSize += p_sizes[i];
            bigChunkSize += calculateLengthFieldSizeAllocBlock(p_sizes[i]);
        }

        ret = multiReserveBlocks(bigChunkSize, p_sizes, p_usedEntries);

        if (ret == null) {
            // fallback to single malloc calls on failure
            ret = new long[p_usedEntries];

            for (int i = 0; i < p_usedEntries; i++) {
                long addr = malloc(p_sizes[i]);

                if (addr == INVALID_ADDRESS) {
                    // roll back
                    for (int j = 0; j < i; j++) {
                        free(ret[j]);
                    }

                    return null;
                }

                ret[i] = addr;
            }
        }

        return ret;
    }

    /**
     * Allocate multiple blocks in a single call. This falls back to normal malloc if the
     * allocator cannot find a single free block that fits all the sizes
     *
     * @param p_size
     *         Size of one block to allocate
     * @param p_count
     *         Number of blocks of p_size each to allocate
     * @return List of addresses for the sizes on success, null on failure
     */
    public long[] multiMalloc(final int p_size, final int p_count) {
        long[] ret;

        if (p_size > m_status.m_maxBlockSize) {
            throw new MemoryRuntimeException("Req allocation size " + p_size + " is exceeding max memory block size " + m_status.m_maxBlockSize);
        }

        if (p_size <= 0) {
            throw new MemoryRuntimeException("Invalid size " + p_size);
        }

        // number of marker bytes to separate blocks
        // -1: one marker byte is already part of the free block
        int bigChunkSize = p_count - 1;

        bigChunkSize += p_size * p_count;
        bigChunkSize += calculateLengthFieldSizeAllocBlock(p_size) * p_count;

        ret = multiReserveBlocks(bigChunkSize, p_size, p_count);

        if (ret == null) {
            // fallback to single malloc calls on failure

            ret = new long[p_count];

            for (int i = 0; i < p_count; i++) {
                long addr = malloc(p_size);

                if (addr == INVALID_ADDRESS) {
                    // roll back
                    for (int j = 0; j < i; j++) {
                        free(ret[j]);
                    }

                    return null;
                }

                ret[i] = addr;
            }
        }

        return ret;
    }

    /**
     * Frees a memory block
     *
     * @param p_address
     *         the address of the block
     */

    public void free(final long p_address) {
        int lengthFieldSize;

        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
        freeReservedBlock(p_address, lengthFieldSize, getSizeMemoryBlock(p_address));
    }

    /**
     * Get the size of an allocated block of memory.
     *
     * @param p_address
     *         Address of the block.
     * @return Size of the block in bytes (payload only).
     */
    public int getSizeBlock(final long p_address) {
        int lengthFieldSize;

        assert assertMemoryBounds(p_address);

        // skip length byte(s)
        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
        return (int) read(p_address, lengthFieldSize);
    }

    /**
     * Overwrites the bytes in the memory with the given value
     *
     * @param p_address
     *         the address to start
     * @param p_size
     *         the number of bytes to overwrite
     * @param p_value
     *         the value to write
     */
    public void set(final long p_address, final long p_size, final byte p_value) {
        assert assertMemoryBounds(p_address, p_size);

        int lengthFieldSize;

        // skip length byte(s)
        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
        m_memory.set(p_address + lengthFieldSize, p_size, p_value);
    }

    /**
     * Read a single byte from the specified address + offset.
     *
     * @param p_address
     *         Address.
     * @param p_offset
     *         Offset to add to the address.
     * @return Byte read.
     */
    public byte readByte(final long p_address, final long p_offset) {
        assert assertMemoryBounds(p_address, p_offset);

        int lengthFieldSize;
        // skip length byte(s)
        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

        assert assertMemoryBlockBounds(p_address, lengthFieldSize, read(p_address, lengthFieldSize), p_offset, Byte.BYTES);

        return m_memory.readByte(p_address + lengthFieldSize + p_offset);
    }

    /**
     * Read a single short from the specified address + offset.
     *
     * @param p_address
     *         Address.
     * @param p_offset
     *         Offset to add to the address.
     * @return Short read.
     */
    public short readShort(final long p_address, final long p_offset) {
        assert assertMemoryBounds(p_address, p_offset);

        int lengthFieldSize;
        // skip length byte(s)
        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

        assert assertMemoryBlockBounds(p_address, lengthFieldSize, read(p_address, lengthFieldSize), p_offset, Short.BYTES);

        return m_memory.readShort(p_address + lengthFieldSize + p_offset);
    }

    /**
     * Read a single int from the specified address + offset.
     *
     * @param p_address
     *         Address.
     * @param p_offset
     *         Offset to add to the address.
     * @return Int read.
     */
    public int readInt(final long p_address, final long p_offset) {
        assert assertMemoryBounds(p_address, p_offset);

        int lengthFieldSize;
        // skip length byte(s)
        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

        assert assertMemoryBlockBounds(p_address, lengthFieldSize, read(p_address, lengthFieldSize), p_offset, Integer.BYTES);

        return m_memory.readInt(p_address + lengthFieldSize + p_offset);
    }

    /**
     * Read a long from the specified address + offset.
     *
     * @param p_address
     *         Address.
     * @param p_offset
     *         Offset to add to the address.
     * @return Long read.
     */
    public long readLong(final long p_address, final long p_offset) {
        assert assertMemoryBounds(p_address, p_offset);

        int lengthFieldSize;
        // skip length byte(s)
        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

        assert assertMemoryBlockBounds(p_address, lengthFieldSize, read(p_address, lengthFieldSize), p_offset, Long.BYTES);

        return m_memory.readLong(p_address + lengthFieldSize + p_offset);
    }

    /**
     * Read data into a byte array.
     *
     * @param p_address
     *         Address in heap to start at.
     * @param p_offset
     *         Offset to add to start address.
     * @param p_buffer
     *         Buffer to read into.
     * @param p_offsetArray
     *         Offset within the buffer.
     * @param p_length
     *         Number of elements to read.
     * @return Number of elements read.
     */
    public int readBytes(final long p_address, final long p_offset, final byte[] p_buffer, final int p_offsetArray, final int p_length) {
        assert assertMemoryBounds(p_address, p_offset);

        int lengthFieldSize;
        // skip length byte(s)
        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

        assert assertMemoryBlockBounds(p_address, lengthFieldSize, read(p_address, lengthFieldSize), p_offset, p_length * Byte.BYTES);

        return m_memory.readBytes(p_address + lengthFieldSize + p_offset, p_buffer, p_offsetArray, p_length);
    }

    /**
     * Read data into a short array.
     *
     * @param p_address
     *         Address in heap to start at.
     * @param p_offset
     *         Offset to add to start address.
     * @param p_buffer
     *         Buffer to read into.
     * @param p_offsetArray
     *         Offset within the buffer.
     * @param p_length
     *         Number of elements to read.
     * @return Number of elements read.
     */
    public int readShorts(final long p_address, final long p_offset, final short[] p_buffer, final int p_offsetArray, final int p_length) {
        assert assertMemoryBounds(p_address, p_offset);

        int lengthFieldSize;
        // skip length byte(s)
        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

        assert assertMemoryBlockBounds(p_address, lengthFieldSize, read(p_address, lengthFieldSize), p_offset, p_length * Short.BYTES);

        return m_memory.readShorts(p_address + lengthFieldSize + p_offset, p_buffer, p_offsetArray, p_length);
    }

    /**
     * Read data into an int array.
     *
     * @param p_address
     *         Address in heap to start at.
     * @param p_offset
     *         Offset to add to start address.
     * @param p_buffer
     *         Buffer to read into.
     * @param p_offsetArray
     *         Offset within the buffer.
     * @param p_length
     *         Number of elements to read.
     * @return Number of elements read.
     */
    public int readInts(final long p_address, final long p_offset, final int[] p_buffer, final int p_offsetArray, final int p_length) {
        assert assertMemoryBounds(p_address, p_offset);

        int lengthFieldSize;
        // skip length byte(s)
        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

        assert assertMemoryBlockBounds(p_address, lengthFieldSize, read(p_address, lengthFieldSize), p_offset, p_length * Integer.BYTES);

        return m_memory.readInts(p_address + lengthFieldSize + p_offset, p_buffer, p_offsetArray, p_length);
    }

    /**
     * Read data into a long array.
     *
     * @param p_address
     *         Address in heap to start at.
     * @param p_offset
     *         Offset to add to start address.
     * @param p_buffer
     *         Buffer to read into.
     * @param p_offsetArray
     *         Offset within the buffer.
     * @param p_length
     *         Number of elements to read.
     * @return Number of elements read.
     */
    public int readLongs(final long p_address, final long p_offset, final long[] p_buffer, final int p_offsetArray, final int p_length) {
        assert assertMemoryBounds(p_address, p_offset);

        int lengthFieldSize;
        // skip length byte(s)
        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

        assert assertMemoryBlockBounds(p_address, lengthFieldSize, read(p_address, lengthFieldSize), p_offset, p_length * Long.BYTES);

        return m_memory.readLongs(p_address + lengthFieldSize + p_offset, p_buffer, p_offsetArray, p_length);
    }

    /**
     * Write a single byte to the specified address + offset.
     *
     * @param p_address
     *         Address.
     * @param p_offset
     *         Offset to add to the address.
     * @param p_value
     *         Byte to write.
     */
    public void writeByte(final long p_address, final long p_offset, final byte p_value) {
        assert assertMemoryBounds(p_address, p_offset);

        int lengthFieldSize;
        // skip length byte(s)
        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

        assert assertMemoryBlockBounds(p_address, lengthFieldSize, read(p_address, lengthFieldSize), p_offset, Byte.BYTES);

        m_memory.writeByte(p_address + lengthFieldSize + p_offset, p_value);
    }

    /**
     * Write a short to the specified address + offset.
     *
     * @param p_address
     *         Address.
     * @param p_offset
     *         Offset to add to the address.
     * @param p_value
     *         Short to write.
     */
    public void writeShort(final long p_address, final long p_offset, final short p_value) {
        assert assertMemoryBounds(p_address, p_offset);

        int lengthFieldSize;
        // skip length byte(s)
        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

        assert assertMemoryBlockBounds(p_address, lengthFieldSize, read(p_address, lengthFieldSize), p_offset, Short.BYTES);

        m_memory.writeShort(p_address + lengthFieldSize + p_offset, p_value);
    }

    /**
     * Write a single int to the specified address + offset.
     *
     * @param p_address
     *         Address.
     * @param p_offset
     *         Offset to add to the address.
     * @param p_value
     *         int to write.
     */
    public void writeInt(final long p_address, final long p_offset, final int p_value) {
        assert assertMemoryBounds(p_address, p_offset);

        int lengthFieldSize;
        // skip length byte(s)
        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

        assert assertMemoryBlockBounds(p_address, lengthFieldSize, read(p_address, lengthFieldSize), p_offset, Integer.BYTES);

        m_memory.writeInt(p_address + lengthFieldSize + p_offset, p_value);
    }

    /**
     * Write a long value to the specified address + offset.
     *
     * @param p_address
     *         Address.
     * @param p_offset
     *         Offset to add to the address.
     * @param p_value
     *         Long value to write.
     */
    public void writeLong(final long p_address, final long p_offset, final long p_value) {
        assert assertMemoryBounds(p_address, p_offset);

        int lengthFieldSize;
        // skip length byte(s)
        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

        assert assertMemoryBlockBounds(p_address, lengthFieldSize, read(p_address, lengthFieldSize), p_offset, Long.BYTES);

        m_memory.writeLong(p_address + lengthFieldSize + p_offset, p_value);
    }

    /**
     * Write an array of bytes to the specified address + offset.
     *
     * @param p_address
     *         Address.
     * @param p_offset
     *         Offset to add to the address.
     * @param p_value
     *         Bytes to write.
     * @param p_offsetArray
     *         Offset within the buffer.
     * @param p_length
     *         Number of elements to read.
     * @return Number of elements written.
     */
    public int writeBytes(final long p_address, final long p_offset, final byte[] p_value, final int p_offsetArray, final int p_length) {
        assert assertMemoryBounds(p_address, p_offset);

        int lengthFieldSize;
        // skip length byte(s)
        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

        assert assertMemoryBlockBounds(p_address, lengthFieldSize, read(p_address, lengthFieldSize), p_offset, Byte.BYTES);

        return m_memory.writeBytes(p_address + lengthFieldSize + p_offset, p_value, p_offsetArray, p_length);
    }

    /**
     * Write bytes from native address to the specified address + offset.
     *
     * @param p_address
     *         Address.
     * @param p_offset
     *         Offset to add to the address.
     * @param p_valueAddress
     *         native address of bytes to write.
     * @param p_valueOffset
     *         Address offset.
     * @param p_length
     *         Number of elements to read.
     * @return Number of elements written.
     */
    public int writeBytes(final long p_address, final long p_offset, final long p_valueAddress, final int p_valueOffset, final int p_length) {
        assert assertMemoryBounds(p_address, p_offset);

        int lengthFieldSize;
        // skip length byte(s)
        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

        assert assertMemoryBlockBounds(p_address, lengthFieldSize, read(p_address, lengthFieldSize), p_offset, Byte.BYTES);

        return m_memory.writeBytes(p_address + lengthFieldSize + p_offset, p_valueAddress, p_valueOffset, p_length);
    }

    /**
     * Write an array of shorts to the heap.
     *
     * @param p_address
     *         Address of an allocated block of memory.
     * @param p_offset
     *         Offset within the block of memory to start at.
     * @param p_value
     *         Array to write.
     * @param p_offsetArray
     *         Offset within the array.
     * @param p_length
     *         Number of elements to write.
     * @return Number of elements written.
     */
    public int writeShorts(final long p_address, final long p_offset, final short[] p_value, final int p_offsetArray, final int p_length) {
        assert assertMemoryBounds(p_address, p_offset);

        int lengthFieldSize;
        // skip length byte(s)
        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

        assert assertMemoryBlockBounds(p_address, lengthFieldSize, read(p_address, lengthFieldSize), p_offset, Short.BYTES);

        return m_memory.writeShorts(p_address + lengthFieldSize + p_offset, p_value, p_offsetArray, p_length);
    }

    // -------------------------------------------------------------------------------------------

    /**
     * Write an array of ints to the heap.
     *
     * @param p_address
     *         Address of an allocated block of memory.
     * @param p_offset
     *         Offset within the block of memory to start at.
     * @param p_value
     *         Array to write.
     * @param p_offsetArray
     *         Offset within the array.
     * @param p_length
     *         Number of elements to write.
     * @return Number of elements written.
     */
    public int writeInts(final long p_address, final long p_offset, final int[] p_value, final int p_offsetArray, final int p_length) {
        assert assertMemoryBounds(p_address, p_offset);

        int lengthFieldSize;
        // skip length byte(s)
        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

        assert assertMemoryBlockBounds(p_address, lengthFieldSize, read(p_address, lengthFieldSize), p_offset, Integer.BYTES);

        return m_memory.writeInts(p_address + lengthFieldSize + p_offset, p_value, p_offsetArray, p_length);
    }

    /**
     * Write an array of longs to the heap.
     *
     * @param p_address
     *         Address of an allocated block of memory.
     * @param p_offset
     *         Offset within the block of memory to start at.
     * @param p_value
     *         Array to write.
     * @param p_offsetArray
     *         Offset within the array.
     * @param p_length
     *         Number of elements to write.
     * @return Number of elements written.
     */
    public int writeLongs(final long p_address, final long p_offset, final long[] p_value, final int p_offsetArray, final int p_length) {
        assert assertMemoryBounds(p_address, p_offset);

        int lengthFieldSize;
        // skip length byte(s)
        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

        assert assertMemoryBlockBounds(p_address, lengthFieldSize, read(p_address, lengthFieldSize), p_offset, Long.BYTES);

        return m_memory.writeLongs(p_address + lengthFieldSize + p_offset, p_value, p_offsetArray, p_length);
    }

    @Override
    public String toString() {
        return "Memory: " + "m_baseFreeBlockList " + m_baseFreeBlockList + ", status: " + m_status;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(m_maxBlockSize);
        p_exporter.writeLong(m_baseFreeBlockList);
        p_exporter.writeInt(m_freeBlocksListSize);
        p_exporter.writeLongArray(m_freeBlockListSizes);
        p_exporter.writeInt(m_freeBlocksListCount);
        p_exporter.exportObject(m_status);
        // separate metadata from VMB with padding
        p_exporter.writeLong(0xFFFFEEDDDDEEFFFFL);

        // write "chunks" of the raw memory to speed up the process
        byte[] buffer = new byte[1024 * 32];

        int chunkSize = buffer.length;
        long ptr = 0;

        while (ptr < m_status.getSize()) {
            if (m_status.getSize() - ptr < chunkSize) {
                chunkSize = (int) (m_status.getSize() - ptr);
            }

            m_memory.readBytes(ptr, buffer, 0, chunkSize);
            p_exporter.writeBytes(buffer, 0, chunkSize);

            ptr += chunkSize;
        }
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_maxBlockSize = p_importer.readInt(m_maxBlockSize);
        m_baseFreeBlockList = p_importer.readLong(m_baseFreeBlockList);
        m_freeBlocksListSize = p_importer.readInt(m_freeBlocksListSize);
        m_freeBlockListSizes = p_importer.readLongArray(m_freeBlockListSizes);
        m_freeBlocksListCount = p_importer.readInt(m_freeBlocksListCount);
        m_status = new Status();
        p_importer.importObject(m_status);
        // get rid of padding separating metadata from VMB
        p_importer.readLong(0);

        // free previously allocated VMB
        m_memory.free();

        // allocate VMB
        m_memory.allocate(m_status.getSize());

        // read "chunks" from file and write to raw memory to speed up the process
        byte[] buffer = new byte[1024 * 32];

        int chunkSize = buffer.length;
        long ptr = 0;

        while (ptr < m_status.getSize()) {
            if (m_status.getSize() - ptr < chunkSize) {
                chunkSize = (int) (m_status.getSize() - ptr);
            }

            p_importer.readBytes(buffer, 0, chunkSize);
            m_memory.writeBytes(ptr, buffer, 0, chunkSize);
            ptr += chunkSize;
        }
    }

    @Override
    public int sizeofObject() {
        throw new UnsupportedOperationException("Heap can be > 2 GB not fitting int type");
        // return (int) (Integer.BYTES + Long.BYTES + Integer.BYTES + ObjectSizeUtil.sizeofLongArray(m_freeBlockListSizes) + Integer.BYTES +
        //     m_status.sizeofObject() + Long.BYTES + m_status.getSize());
    }

    /**
     * Reads up to 8 bytes combined in a long
     *
     * @param p_address
     *         the address
     * @param p_count
     *         the number of bytes
     * @return the combined bytes
     */
    long read(final long p_address, final int p_count) {
        return m_memory.readVal(p_address, p_count);
    }

    /**
     * Read the right part of a marker byte
     *
     * @param p_address
     *         the address
     * @return the right part of a marker byte
     */
    int readRightPartOfMarker(final long p_address) {
        return m_memory.readByte(p_address) & 0xF;
    }

    /**
     * Read the left part of a marker byte
     *
     * @param p_address
     *         the address
     * @return the left part of a marker byte
     */
    int readLeftPartOfMarker(final long p_address) {
        return (m_memory.readByte(p_address) & 0xF0) >> 4;
    }

    /**
     * Reads a pointer
     *
     * @param p_address
     *         the address
     * @return the pointer
     */
    long readPointer(final long p_address) {
        return read(p_address, POINTER_SIZE);
    }

    /**
     * Get the size of the allocated or free'd block of memory specified
     * by the given address.
     *
     * @param p_address
     *         Address of block to get the size of.
     * @return Size of memory block at specified address.
     */
    private long getSizeMemoryBlock(final long p_address) {
        int lengthFieldSize;

        assert assertMemoryBounds(p_address);

        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
        return read(p_address, lengthFieldSize);
    }

    /**
     * Reserve a free block of memory.
     *
     * @param p_size
     *         Size of the block (payload size).
     * @return Address of the reserved block or null if out of memory of no block for requested size was found.
     */
    private long reserveBlock(final int p_size) {
        assert p_size > 0;

        long address;
        int blockSize;
        int lengthFieldSize;
        byte blockMarker;

        if (p_size > m_status.m_maxBlockSize) {
            throw new MemoryRuntimeException("Req allocation size " + p_size + " is exceeding max memory block size " + m_status.m_maxBlockSize);
        }

        lengthFieldSize = calculateLengthFieldSizeAllocBlock(p_size);

        blockMarker = (byte) (ALLOC_BLOCK_FLAGS_OFFSET + lengthFieldSize);
        blockSize = p_size + lengthFieldSize;
        address = findFreeBlock(blockSize);

        if (address != INVALID_ADDRESS) {
            unhookFreeBlock(address);
            trimFreeBlockToSize(address, blockSize);

            // Write marker
            writeLeftPartOfMarker(address + blockSize, blockMarker);
            writeRightPartOfMarker(address - SIZE_MARKER_BYTE, blockMarker);

            write(address, p_size, lengthFieldSize);

            m_status.m_allocatedPayload += p_size;
            m_status.m_allocatedBlocks++;
        }

        return address;
    }

    /**
     * Find a free block with a minimum size
     *
     * @param p_size
     *         Number of bytes that have to fit into that block
     * @return Address of the still hooked free block
     */
    private long findFreeBlock(final int p_size) {
        int list;
        int listIdx;
        long address;
        long freeSize;
        int freeLengthFieldSize;

        // Get the list with a free block which is big enough
        list = getList(p_size) + 1;
        while (list < m_freeBlocksListCount && readPointer(m_baseFreeBlockList + list * POINTER_SIZE) == 0) {
            list++;
        }
        if (list < m_freeBlocksListCount) {
            // A list is found
            address = readPointer(m_baseFreeBlockList + list * POINTER_SIZE);
        } else {
            // Traverse through the lower list
            list = getList(p_size);
            address = readPointer(m_baseFreeBlockList + list * POINTER_SIZE);
            if (address != INVALID_ADDRESS) {
                freeLengthFieldSize = readRightPartOfMarker(address - 1);
                freeSize = read(address, freeLengthFieldSize);
                while (freeSize < p_size && address != INVALID_ADDRESS) {
                    address = readPointer(address + freeLengthFieldSize + POINTER_SIZE);
                    if (address != INVALID_ADDRESS) {
                        freeLengthFieldSize = readRightPartOfMarker(address - 1);
                        freeSize = read(address, freeLengthFieldSize);
                    }
                }
            }
        }

        return address;
    }

    /**
     * Uses an unhooked block and trims it to the right size to exactly fit the
     * specified number of bytes. The unused space is hooked back as free space.
     *
     * @param p_address
     *         Address of the unhooked block to trim
     * @param p_size
     *         Size to trim the block to
     */
    private void trimFreeBlockToSize(final long p_address, final long p_size) {
        long freeSize;
        int freeLengthFieldSize;

        freeLengthFieldSize = readRightPartOfMarker(p_address - SIZE_MARKER_BYTE);
        freeSize = read(p_address, freeLengthFieldSize);
        if (freeSize == p_size) {
            m_status.m_free -= p_size;
            m_status.m_freeBlocks--;
            if (freeSize < SMALL_BLOCK_SIZE) {
                m_status.m_freeSmall64ByteBlocks--;
            }
        } else if (freeSize == p_size + 1) {
            // 1 Byte to big -> write two markers on the right
            writeRightPartOfMarker(p_address + p_size, SINGLE_BYTE_MARKER);
            writeLeftPartOfMarker(p_address + p_size + 1, SINGLE_BYTE_MARKER);

            // +1 for the marker byte added
            m_status.m_free -= p_size + 1;
            m_status.m_freeBlocks--;
            if (freeSize + 1 < SMALL_BLOCK_SIZE) {
                m_status.m_freeSmall64ByteBlocks--;
            }
        } else {
            // Block is too big -> create a new free block with the remaining size
            createFreeBlock(p_address + p_size + 1, freeSize - p_size - 1);

            // +1 for the marker byte added
            m_status.m_free -= p_size + 1;

            if (freeSize >= SMALL_BLOCK_SIZE && freeSize - p_size - 1 < SMALL_BLOCK_SIZE) {
                m_status.m_freeSmall64ByteBlocks++;
            }
        }
    }

    /**
     * Reserve multiple blocks with a single call reducing metadata processing overhead
     *
     * @param p_bigBlockSize
     *         Total size of the block to reserve. This already needs to include all marker bytes and length fields aside the payload sizes
     * @param p_sizes
     *         List of block sizes (payloads, only)
     * @return Addresses of the allocated blocks
     */
    private long[] multiReserveBlocks(final int p_bigBlockSize, final int[] p_sizes, final int p_usedEntries) {
        long[] ret;
        int size;
        long address;
        int lengthFieldSize;
        byte blockMarker;

        address = findFreeBlock(p_bigBlockSize);

        // no free block found
        if (address == INVALID_ADDRESS) {
            return null;
        }

        unhookFreeBlock(address);
        trimFreeBlockToSize(address, p_bigBlockSize);

        ret = new long[p_usedEntries];

        for (int i = 0; i < p_usedEntries; i++) {
            size = p_sizes[i];

            if (size > m_status.m_maxBlockSize) {
                throw new MemoryRuntimeException("Req allocation size " + size + " is exceeding max memory block size " + m_status.m_maxBlockSize);
            }

            lengthFieldSize = calculateLengthFieldSizeAllocBlock(size);

            blockMarker = (byte) (ALLOC_BLOCK_FLAGS_OFFSET + lengthFieldSize);

            writeRightPartOfMarker(address - SIZE_MARKER_BYTE, blockMarker);
            writeLeftPartOfMarker(address + lengthFieldSize + size, blockMarker);
            write(address, size, lengthFieldSize);

            ret[i] = address;

            // +1 : right side marker byte
            address += lengthFieldSize + size + 1;

            // update full size
            m_status.m_allocatedPayload += p_sizes[i];
            m_status.m_allocatedBlocks++;
        }

        return ret;
    }

    /**
     * Reserve multiple blocks with a single call reducing metadata processing overhead
     *
     * @param p_bigBlockSize
     *         Total size of the block to reserve. This already needs to include all marker bytes and length fields aside the payload sizes
     * @param p_size
     *         Size of the block
     * @param p_count
     *         Number of blocks of p_size each
     * @return Addresses of the allocated blocks
     */
    private long[] multiReserveBlocks(final int p_bigBlockSize, final int p_size, final int p_count) {
        long[] ret;
        int size;
        long address;
        int lengthFieldSize;
        byte blockMarker;

        address = findFreeBlock(p_bigBlockSize);

        // no free block found
        if (address == INVALID_ADDRESS) {
            return null;
        }

        unhookFreeBlock(address);
        trimFreeBlockToSize(address, p_bigBlockSize);

        ret = new long[p_count];

        for (int i = 0; i < p_count; i++) {
            size = p_size;

            if (size > m_status.m_maxBlockSize) {
                throw new MemoryRuntimeException("Req allocation size " + size + " is exceeding max memory block size " + m_status.m_maxBlockSize);
            }

            lengthFieldSize = calculateLengthFieldSizeAllocBlock(size);

            blockMarker = (byte) (ALLOC_BLOCK_FLAGS_OFFSET + lengthFieldSize);

            writeRightPartOfMarker(address - SIZE_MARKER_BYTE, blockMarker);
            writeLeftPartOfMarker(address + lengthFieldSize + size, blockMarker);
            write(address, size, lengthFieldSize);

            ret[i] = address;

            // +1 : right side marker byte
            address += lengthFieldSize + size + 1;

            // update full size
            m_status.m_allocatedPayload += p_size;
            m_status.m_allocatedBlocks++;
        }

        return ret;
    }

    /**
     * Free a reserved block of memory
     *
     * @param p_address
     *         Address of the block
     * @param p_lengthFieldSize
     *         Size of the length field
     * @param p_blockSize
     *         Size of the block's payload
     */
    private void freeReservedBlock(final long p_address, final int p_lengthFieldSize, final long p_blockSize) {
        long blockSize;
        long freeSize;
        long address;
        int lengthFieldSize;
        int leftMarker;
        int rightMarker;
        boolean leftFree;
        long leftSize;
        boolean rightFree;
        long rightSize;

        assert assertMemoryBounds(p_address);

        blockSize = p_blockSize;
        lengthFieldSize = p_lengthFieldSize;
        freeSize = blockSize + lengthFieldSize;
        address = p_address;

        // only merge if left neighbor exists (beginning of memory area)
        if (address - SIZE_MARKER_BYTE != 0) {
            // Read left part of the marker on the left
            leftMarker = readLeftPartOfMarker(address - 1);
            leftFree = true;
            switch (leftMarker) {
                case 0:
                    // Left neighbor block (<= 12 byte) is free -> merge free blocks
                    // -1, length field size is 1
                    leftSize = read(address - SIZE_MARKER_BYTE - 1, 1);
                    // merge marker byte
                    leftSize += SIZE_MARKER_BYTE;
                    break;
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                    // Left neighbor block is free -> merge free blocks
                    leftSize = read(address - SIZE_MARKER_BYTE - leftMarker, leftMarker);
                    // skip leftSize and marker byte from address to get block offset
                    unhookFreeBlock(address - leftSize - SIZE_MARKER_BYTE);
                    // we also merge the marker byte
                    leftSize += SIZE_MARKER_BYTE;
                    break;
                case SINGLE_BYTE_MARKER:
                    // Left byte is free -> merge free blocks
                    leftSize = 1;
                    break;
                default:
                    leftSize = 0;
                    leftFree = false;
                    break;
            }
        } else {
            leftSize = 0;
            leftFree = false;
        }

        // update start address of free block and size
        address -= leftSize;
        freeSize += leftSize;

        // Only merge if right neighbor within valid area (not inside or past free blocks list)
        if (address + blockSize + SIZE_MARKER_BYTE != m_baseFreeBlockList) {

            // Read right part of the marker on the right
            rightMarker = readRightPartOfMarker(p_address + lengthFieldSize + blockSize);
            rightFree = true;
            switch (rightMarker) {
                case 0:
                    // Right neighbor block (<= 12 byte) is free -> merge free blocks
                    // + 1 to skip marker byte
                    rightSize = read(p_address + lengthFieldSize + blockSize + SIZE_MARKER_BYTE, 1);
                    // merge marker byte
                    rightSize += SIZE_MARKER_BYTE;
                    break;
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                    // Right neighbor block is free -> merge free blocks
                    // + 1 to skip marker byte
                    rightSize = getSizeMemoryBlock(p_address + lengthFieldSize + blockSize + SIZE_MARKER_BYTE);
                    unhookFreeBlock(p_address + lengthFieldSize + blockSize + SIZE_MARKER_BYTE);
                    // we also merge the marker byte
                    rightSize += SIZE_MARKER_BYTE;
                    break;
                case 15:
                    // Right byte is free -> merge free blocks
                    rightSize = 1;
                    break;
                default:
                    rightSize = 0;
                    rightFree = false;
                    break;
            }
        } else {
            rightSize = 0;
            rightFree = false;
        }

        // update size of full free block
        freeSize += rightSize;

        // Create a free block
        createFreeBlock(address, freeSize);

        if (!leftFree && !rightFree) {
            m_status.m_free += blockSize + lengthFieldSize;
            m_status.m_freeBlocks++;
            if (blockSize + lengthFieldSize < SMALL_BLOCK_SIZE) {
                m_status.m_freeSmall64ByteBlocks++;
            }
        } else if (leftFree && !rightFree) {
            m_status.m_free += blockSize + lengthFieldSize + SIZE_MARKER_BYTE;
            if (blockSize + lengthFieldSize + leftSize >= SMALL_BLOCK_SIZE && leftSize < SMALL_BLOCK_SIZE) {
                m_status.m_freeSmall64ByteBlocks--;
            }
        } else if (!leftFree /*&& rightFree*/) {
            m_status.m_free += blockSize + lengthFieldSize + SIZE_MARKER_BYTE;
            if (blockSize + lengthFieldSize + rightSize >= SMALL_BLOCK_SIZE && rightSize < SMALL_BLOCK_SIZE) {
                m_status.m_freeSmall64ByteBlocks--;
            }
            // leftFree && rightFree
        } else {
            // +2 for two marker bytes being merged
            m_status.m_free += blockSize + lengthFieldSize + 2 * SIZE_MARKER_BYTE;
            m_status.m_freeBlocks--;
            if (blockSize + lengthFieldSize + leftSize + rightSize >= SMALL_BLOCK_SIZE) {
                if (rightSize < SMALL_BLOCK_SIZE && leftSize < SMALL_BLOCK_SIZE) {
                    m_status.m_freeSmall64ByteBlocks--;
                } else if (rightSize >= SMALL_BLOCK_SIZE && leftSize >= SMALL_BLOCK_SIZE) {
                    m_status.m_freeSmall64ByteBlocks++;
                }
            }
        }

        m_status.m_allocatedPayload -= blockSize;
        m_status.m_allocatedBlocks--;
    }

    /**
     * Check the memory bounds with the specified address.
     *
     * @param p_address
     *         Address to check if within memory.
     * @return Dummy return for assert
     */
    private boolean assertMemoryBounds(final long p_address) {
        if (p_address < 0 || p_address > m_status.m_size) {
            throw new MemoryRuntimeException("Address " + p_address + " is not within memory: " + this);
        }

        return true;
    }

    /**
     * Check the memory bounds with the specified start address and size.
     *
     * @param p_address
     *         Address to check if within bounds.
     * @param p_length
     *         Number of bytes starting at address.
     * @return Dummy return for assert
     */
    private boolean assertMemoryBounds(final long p_address, final long p_length) {
        if (p_address < 0 || p_address > m_status.m_size || p_address + p_length < 0 || p_address + p_length > m_status.m_size) {
            throw new MemoryRuntimeException("Address " + p_address + " with length " + p_length + "is not within memory: " + this);
        }

        return true;
    }

    /**
     * Creates a free block
     *
     * @param p_address
     *         the address
     * @param p_size
     *         the size
     */
    private void createFreeBlock(final long p_address, final long p_size) {
        long listOffset;
        int lengthFieldSize;
        long anchor;
        long size;

        if (p_size < 12) {
            // If size < 12 -> the block will not be hook in the lists
            lengthFieldSize = 0;

            write(p_address, p_size, 1);
            write(p_address + p_size - 1, p_size, 1);
        } else {
            lengthFieldSize = 1;

            // Calculate the number of bytes for the length field
            size = p_size >> 8;
            while (size > 0) {
                lengthFieldSize++;

                size >>= 8;
            }

            // Get the corresponding list
            listOffset = m_baseFreeBlockList + getList(p_size) * POINTER_SIZE;

            // Hook block in list
            anchor = readPointer(listOffset);

            // Write pointer to list and successor
            writePointer(p_address + lengthFieldSize, listOffset);
            writePointer(p_address + lengthFieldSize + POINTER_SIZE, anchor);
            if (anchor != INVALID_ADDRESS) {
                // Write pointer of successor
                int marker;
                marker = readRightPartOfMarker(anchor - SIZE_MARKER_BYTE);
                writePointer(anchor + marker, p_address);
            }
            // Write pointer of list
            writePointer(listOffset, p_address);

            // Write length
            write(p_address, p_size, lengthFieldSize);
            write(p_address + p_size - lengthFieldSize, p_size, lengthFieldSize);
        }

        // Write right and left marker
        writeRightPartOfMarker(p_address - SIZE_MARKER_BYTE, lengthFieldSize);
        writeLeftPartOfMarker(p_address + p_size, lengthFieldSize);
    }

    /**
     * Unhooks a free block
     *
     * @param p_address
     *         the address
     */
    private void unhookFreeBlock(final long p_address) {
        int lengthFieldSize;
        long prevPointer;
        long nextPointer;

        // Read size of length field
        lengthFieldSize = readRightPartOfMarker(p_address - SIZE_MARKER_BYTE);

        // Read pointers
        prevPointer = readPointer(p_address + lengthFieldSize);
        nextPointer = readPointer(p_address + lengthFieldSize + POINTER_SIZE);

        if (prevPointer >= m_baseFreeBlockList) {
            // Write Pointer of list
            writePointer(prevPointer, nextPointer);
        } else {
            // Write Pointer of predecessor
            writePointer(prevPointer + lengthFieldSize + POINTER_SIZE, nextPointer);
        }

        if (nextPointer != INVALID_ADDRESS) {
            // Write pointer of successor
            writePointer(nextPointer + lengthFieldSize, prevPointer);
        }
    }

    /**
     * Gets the suitable list for the given size
     *
     * @param p_size
     *         the size
     * @return Index of the suitable list
     */
    private int getList(final long p_size) {
        int ret = 0;

        while (ret + 1 < m_freeBlockListSizes.length && m_freeBlockListSizes[ret + 1] <= p_size) {
            ret++;
        }

        return ret;
    }

    /**
     * Writes a marker byte
     *
     * @param p_address
     *         the address
     * @param p_right
     *         the right part
     */
    private void writeRightPartOfMarker(final long p_address, final int p_right) {
        byte marker;

        marker = (byte) ((m_memory.readByte(p_address) & 0xF0) + (p_right & 0xF));
        m_memory.writeByte(p_address, marker);
    }

    /**
     * Writes a marker byte
     *
     * @param p_address
     *         the address
     * @param p_left
     *         the left part
     */
    private void writeLeftPartOfMarker(final long p_address, final int p_left) {
        byte marker;

        marker = (byte) (((p_left & 0xF) << 4) + (m_memory.readByte(p_address) & 0xF));
        m_memory.writeByte(p_address, marker);
    }

    /**
     * Writes a pointer
     *
     * @param p_address
     *         the address
     * @param p_pointer
     *         the pointer
     */
    private void writePointer(final long p_address, final long p_pointer) {
        write(p_address, p_pointer, POINTER_SIZE);
    }

    /**
     * Writes up to 8 bytes combined in a long
     *
     * @param p_address
     *         the address
     * @param p_bytes
     *         the combined bytes
     * @param p_count
     *         the number of bytes
     */
    private void write(final long p_address, final long p_bytes, final int p_count) {
        m_memory.writeVal(p_address, p_bytes, p_count);
    }

    // --------------------------------------------------------------------------------------

    // Classes

    /**
     * Holds fragmentation information of the memory
     *
     * @author Florian Klein 10.04.2014
     */
    public static final class Status implements Importable, Exportable {

        private long m_size;
        private int m_maxBlockSize;
        private long m_free;
        private long m_allocatedPayload;
        private long m_allocatedBlocks;
        private long m_freeBlocks;
        private long m_freeSmall64ByteBlocks;

        /**
         * Get the total size of the memory
         *
         * @return Total size in bytes
         */
        public long getSize() {
            return m_size;
        }

        /**
         * Get the maximum size for a single memory block
         *
         * @return Max size for a single memory block
         */
        public int getMaxBlockSize() {
            return m_maxBlockSize;
        }

        /**
         * Get the total amount of free memory
         *
         * @return Total free memory in bytes
         */
        public long getFree() {
            return m_free;
        }

        /**
         * Get the total amount of bytes used for payload of the allocated blocks
         *
         * @return Amount of memory in bytes used for payload
         */
        public long getAllocatedPayload() {
            return m_allocatedPayload;
        }

        /**
         * Get the total number of allocated blocks
         *
         * @return Number of allocated blocks
         */
        public long getAllocatedBlocks() {
            return m_allocatedBlocks;
        }

        /**
         * Get the total number of free blocks
         *
         * @return Number of free blocks
         */
        public long getFreeBlocks() {
            return m_freeBlocks;
        }

        /**
         * Get the total number of free blocks with a size of less than 64 bytes
         *
         * @return Number of small free blocks
         */
        public long getFreeSmall64ByteBlocks() {
            return m_freeSmall64ByteBlocks;
        }

        /**
         * Gets the current fragmentation in percentage
         *
         * @return the fragmentation
         */
        public double getFragmentation() {
            double ret = 0;

            if (m_freeSmall64ByteBlocks >= 1 || m_freeBlocks >= 1) {
                ret = (double) m_freeSmall64ByteBlocks / m_freeBlocks;
            }

            return ret;
        }

        @Override
        public String toString() {
            return "Status [m_size=" + m_size + ", m_free=" + m_free + ", m_allocatedPayload=" + m_allocatedPayload + ", m_allocatedBlocks=" +
                    m_allocatedBlocks + ", m_freeBlocks=" + m_freeBlocks + ", m_freeSmall64ByteBlocks=" + m_freeSmall64ByteBlocks + ", fragmentation=" +
                    getFragmentation() + ']';
        }

        @Override
        public void exportObject(final Exporter p_exporter) {
            p_exporter.writeLong(m_size);
            p_exporter.writeInt(m_maxBlockSize);
            p_exporter.writeLong(m_free);
            p_exporter.writeLong(m_allocatedPayload);
            p_exporter.writeLong(m_allocatedBlocks);
            p_exporter.writeLong(m_freeBlocks);
            p_exporter.writeLong(m_freeSmall64ByteBlocks);
        }

        @Override
        public void importObject(final Importer p_importer) {
            m_size = p_importer.readLong(m_size);
            m_maxBlockSize = p_importer.readInt(m_maxBlockSize);
            m_free = p_importer.readLong(m_free);
            m_allocatedPayload = p_importer.readLong(m_allocatedPayload);
            m_allocatedBlocks = p_importer.readLong(m_allocatedBlocks);
            m_freeBlocks = p_importer.readLong(m_freeBlocks);
            m_freeSmall64ByteBlocks = p_importer.readLong(m_freeSmall64ByteBlocks);
        }

        @Override
        public int sizeofObject() {
            return Long.BYTES + Integer.BYTES + Long.BYTES * 5;
        }
    }

}
