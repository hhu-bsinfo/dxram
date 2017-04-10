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

package de.hhu.bsinfo.soh;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Gathers data from a SmallObjectHeap for further (error) analysis
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.11.2015
 */
public final class SmallObjectHeapAnalyzer {

    private SmallObjectHeap m_memory;

    /**
     * Constructor
     *
     * @param p_heap
     *     Heap to analyze
     */
    public SmallObjectHeapAnalyzer(final SmallObjectHeap p_heap) {
        m_memory = p_heap;
    }

    /**
     * Analyze heap and output only errors to stdout
     */
    public void analyzeErrorsOnly() {
        walkMemoryBlocks(null);
        walkMemoryFreeBlockList(null);
    }

    /**
     * Analyze the heap and return detailed information
     *
     * @return Heap information
     */
    public Heap analyze() {
        Heap results = new Heap();
        results.m_totalSize = m_memory.m_memory.getSize();

        walkMemoryBlocks(results);
        walkMemoryFreeBlockList(results);

        return results;
    }

    /**
     * Walk the memory blocks of a segment
     *
     * @param p_heap
     *     Heap information
     */
    private void walkMemoryBlocks(final Heap p_heap) {
        long baseAddress;
        long blockAreaSize;

        // get what we need from the segment
        baseAddress = 0;
        // size = total - last section that holds the free block list roots
        blockAreaSize = m_memory.m_baseFreeBlockList;

        // walk memory block area
        while (baseAddress < blockAreaSize - 1) {
            MemoryBlock block;
            block = new MemoryBlock();

            block.m_startAddress = baseAddress;
            block.m_markerByte = m_memory.readRightPartOfMarker(baseAddress);

            switch (block.m_markerByte) {
                // free memory less than 12 bytes
                case 0: {
                    int lengthFieldSize;
                    int sizeBlock;

                    lengthFieldSize = 1;
                    // size includes length field
                    sizeBlock = (int) m_memory.read(baseAddress + SmallObjectHeap.SIZE_MARKER_BYTE, lengthFieldSize);

                    // check actual size in range
                    if (sizeBlock >= 12 || sizeBlock <= 1) {
                        block.m_error = MemoryBlock.ERROR.INVALID_LENGTH_FIELD_CONTENTS;
                        block.m_errorText = Integer.toString(sizeBlock);
                    }

                    // + 2 half marker bytes
                    block.m_endAddress = baseAddress + sizeBlock + SmallObjectHeap.SIZE_MARKER_BYTE;
                    block.m_rawBlockSize = sizeBlock;
                    block.m_prevFreeBlock = -1;
                    block.m_nextFreeBlock = -1;
                    block.m_blockPayloadSize = -1;

                    // check end marker byte
                    if (block.m_markerByte != m_memory.readLeftPartOfMarker(block.m_endAddress)) {
                        block.m_error = MemoryBlock.ERROR.MARKER_BYTES_NOT_MATCHING;
                        block.m_errorText = "0x" + Integer.toHexString(block.m_markerByte) + " != 0x" +
                            Integer.toHexString(m_memory.readLeftPartOfMarker(block.m_endAddress - 1));
                    }

                    // proceed
                    baseAddress += SmallObjectHeap.SIZE_MARKER_BYTE + lengthFieldSize;
                    break;
                }

                // free block with X bytes length field and size >= 12 bytes
                case 1:
                case 2:
                case 3:
                case 4:
                case 5: {
                    int lengthFieldSize;
                    long freeBlockSize;

                    lengthFieldSize = block.m_markerByte;
                    // size includes length field already
                    freeBlockSize = m_memory.read(baseAddress + SmallObjectHeap.SIZE_MARKER_BYTE, lengthFieldSize);

                    // + 2 half marker bytes
                    block.m_endAddress = baseAddress + freeBlockSize + SmallObjectHeap.SIZE_MARKER_BYTE;
                    block.m_rawBlockSize = freeBlockSize;
                    block.m_prevFreeBlock = m_memory.readPointer(baseAddress + SmallObjectHeap.SIZE_MARKER_BYTE + lengthFieldSize);
                    if (block.m_prevFreeBlock < 0 || block.m_prevFreeBlock >= blockAreaSize) {

                        // prev free block can be the root of the free memory linked list at the end after the VMB area
                        if (!(block.m_prevFreeBlock >= m_memory.m_baseFreeBlockList && block.m_prevFreeBlock < m_memory.getStatus().getSize())) {
                            block.m_error = MemoryBlock.ERROR.INVALID_POINTERS;
                            block.m_errorText = "0x" + Long.toHexString(block.m_prevFreeBlock);
                        }
                    }

                    block.m_nextFreeBlock = m_memory.readPointer(baseAddress + lengthFieldSize + SmallObjectHeap.POINTER_SIZE);
                    if (block.m_nextFreeBlock < 0 || block.m_nextFreeBlock >= blockAreaSize) {
                        block.m_error = MemoryBlock.ERROR.INVALID_POINTERS;
                        block.m_errorText = "0x" + Long.toHexString(block.m_nextFreeBlock);
                    }

                    // check end marker byte
                    if (block.m_markerByte != m_memory.readLeftPartOfMarker(block.m_endAddress)) {
                        block.m_error = MemoryBlock.ERROR.MARKER_BYTES_NOT_MATCHING;
                        block.m_errorText = "0x" + Integer.toHexString(block.m_markerByte) + " != 0x" +
                            Integer.toHexString(m_memory.readLeftPartOfMarker(block.m_endAddress - 1));
                    }

                    // check actual size in range
                    switch (block.m_markerByte) {
                        case 1:
                            if (!(freeBlockSize >= 12 && freeBlockSize <= 0xFF)) {
                                block.m_error = MemoryBlock.ERROR.INVALID_LENGTH_FIELD_CONTENTS;
                                block.m_errorText = Long.toString(freeBlockSize);
                            }
                            break;

                        case 2:
                            if (!(freeBlockSize > 0xFF && freeBlockSize <= 0xFFFF)) {
                                block.m_error = MemoryBlock.ERROR.INVALID_LENGTH_FIELD_CONTENTS;
                                block.m_errorText = Long.toString(freeBlockSize);
                            }
                            break;

                        case 3:
                            if (!(freeBlockSize > 0xFFFF && freeBlockSize <= 0xFFFFFF)) {
                                block.m_error = MemoryBlock.ERROR.INVALID_LENGTH_FIELD_CONTENTS;
                                block.m_errorText = Long.toString(freeBlockSize);
                            }
                            break;

                        case 4:
                            if (!(freeBlockSize > 0xFFFFFF && freeBlockSize <= 0xFFFFFFFFL)) {
                                block.m_error = MemoryBlock.ERROR.INVALID_LENGTH_FIELD_CONTENTS;
                                block.m_errorText = Long.toString(freeBlockSize);
                            }
                            break;

                        case 5:
                            if (!(freeBlockSize > 0xFFFFFFFFL && freeBlockSize <= 0xFFFFFFFFFFL)) {
                                block.m_error = MemoryBlock.ERROR.INVALID_LENGTH_FIELD_CONTENTS;
                                block.m_errorText = Long.toString(freeBlockSize);
                            }
                            break;

                        default:
                            break;
                    }

                    // proceed
                    baseAddress += SmallObjectHeap.SIZE_MARKER_BYTE + freeBlockSize;

                    break;
                }

                // malloc'd block, 1, 2, 3, 4 byte length field
                case 6:
                case 7:
                case 8:
                case 9: {
                    int lengthFieldSize;
                    long blockPayloadSize;

                    lengthFieldSize = block.m_markerByte - SmallObjectHeap.ALLOC_BLOCK_FLAGS_OFFSET;
                    blockPayloadSize = m_memory.read(baseAddress + SmallObjectHeap.SIZE_MARKER_BYTE, lengthFieldSize);

                    // check actual size in range
                    switch (lengthFieldSize) {
                        case 1:
                            if (!(blockPayloadSize >= 12 && blockPayloadSize <= 0xFF)) {
                                block.m_error = MemoryBlock.ERROR.INVALID_LENGTH_FIELD_CONTENTS;
                                block.m_errorText = Long.toString(blockPayloadSize);
                            }
                            break;

                        case 2:
                            if (!(blockPayloadSize > 0xFF && blockPayloadSize <= 0xFFFF)) {
                                block.m_error = MemoryBlock.ERROR.INVALID_LENGTH_FIELD_CONTENTS;
                                block.m_errorText = Long.toString(blockPayloadSize);
                            }
                            break;

                        case 3:
                            if (!(blockPayloadSize > 0xFFFF && blockPayloadSize <= 0xFFFFFF)) {
                                block.m_error = MemoryBlock.ERROR.INVALID_LENGTH_FIELD_CONTENTS;
                                block.m_errorText = Long.toString(blockPayloadSize);
                            }
                            break;

                        case 4:
                            if (!(blockPayloadSize > 0xFFFFFF && blockPayloadSize <= 0xFFFFFFFF)) {
                                block.m_error = MemoryBlock.ERROR.INVALID_LENGTH_FIELD_CONTENTS;
                                block.m_errorText = Long.toString(blockPayloadSize);
                            }
                            break;

                        default:
                            break;
                    }

                    // + 2 half marker bytes
                    block.m_endAddress = baseAddress + lengthFieldSize + blockPayloadSize + SmallObjectHeap.SIZE_MARKER_BYTE;
                    block.m_rawBlockSize = lengthFieldSize + blockPayloadSize;
                    block.m_prevFreeBlock = -1;
                    block.m_nextFreeBlock = -1;
                    block.m_blockPayloadSize = blockPayloadSize;

                    // check end marker byte
                    if (block.m_markerByte != m_memory.readLeftPartOfMarker(block.m_endAddress)) {
                        block.m_error = MemoryBlock.ERROR.MARKER_BYTES_NOT_MATCHING;
                        block.m_errorText = "0x" + Integer.toHexString(block.m_markerByte) + " != 0x" +
                            Integer.toHexString(m_memory.readLeftPartOfMarker(block.m_endAddress - 1));
                    }

                    // proceed
                    baseAddress += SmallObjectHeap.SIZE_MARKER_BYTE + lengthFieldSize + blockPayloadSize;

                    break;
                }

                // free memory 1 byte
                case 15: {
                    block.m_endAddress = baseAddress + SmallObjectHeap.SIZE_MARKER_BYTE;
                    block.m_rawBlockSize = 1;
                    block.m_prevFreeBlock = -1;
                    block.m_nextFreeBlock = -1;
                    block.m_blockPayloadSize = -1;

                    // proceed
                    baseAddress += SmallObjectHeap.SIZE_MARKER_BYTE;

                    break;
                }

                default: {
                    block.m_error = MemoryBlock.ERROR.INVALID_MARKER_BYTE;
                    block.m_errorText = Integer.toHexString(block.m_markerByte);
                    break;
                }
            }

            if (p_heap != null) {
                p_heap.m_memoryBlocks.put(block.m_startAddress, block);
            }

            if (block.isCorrupted()) {
                if (p_heap != null) {
                    p_heap.m_isCorrupted = true;
                }

                System.out.println(block);
            }
        }
    }

    /**
     * Walk the free block list of a segment
     *
     * @param p_heap
     *     Heap information
     */
    private void walkMemoryFreeBlockList(final Heap p_heap) {
        long baseAddress;
        long freeBlockListAreaSize;
        long freeBlockListEnd;
        long[] freeBlockListSizes;

        // get what we need from the segment
        baseAddress = m_memory.m_baseFreeBlockList;
        freeBlockListAreaSize = m_memory.m_freeBlocksListSize;
        freeBlockListEnd = baseAddress + freeBlockListAreaSize;
        freeBlockListSizes = m_memory.m_freeBlockListSizes;

        for (int i = 0; i < freeBlockListSizes.length; i++) {
            FreeBlockList list = null;

            if (p_heap != null) {
                list = new FreeBlockList();
                p_heap.m_freeBlockLists.add(list);
                list.m_minFreeBlockSize = freeBlockListSizes[i];
                list.m_addressRoot = baseAddress;
            }

            if (baseAddress <= freeBlockListEnd) {
                long ptr = m_memory.readPointer(baseAddress);

                if (ptr != SmallObjectHeap.INVALID_ADDRESS) {

                    do {
                        FreeBlockListElement block = new FreeBlockListElement();

                        int marker;
                        marker = m_memory.readRightPartOfMarker(ptr - 1);

                        // verify marker byte of memory block first
                        switch (marker) {
                            case 1:
                            case 2:
                            case 3:
                            case 4:
                            case 5: {
                                int lengthFieldSize;
                                long blockSize;
                                long ptrPrev;
                                long ptrNext;

                                lengthFieldSize = marker;
                                blockSize = m_memory.read(ptr, marker);

                                // sanity check block size
                                if (blockSize < 12) {
                                    block.m_error = FreeBlockListElement.ERROR.INVALID_BLOCK_SIZE;
                                    block.m_errorText = Long.toString(blockSize);
                                }

                                ptrPrev = m_memory.readPointer(ptr + lengthFieldSize);
                                if (ptrPrev < 0 || ptrPrev >= m_memory.m_baseFreeBlockList) {

                                    // prev free block can be the root of the free memory linked list at the end after the VMB area
                                    if (!(ptrPrev >= m_memory.m_baseFreeBlockList && ptrPrev < m_memory.getStatus().getSize())) {
                                        block.m_error = FreeBlockListElement.ERROR.INVALID_POINTERS;
                                        block.m_errorText = "0x" + Long.toHexString(ptrPrev);
                                    }
                                }

                                ptrNext = m_memory.readPointer(ptr + lengthFieldSize + SmallObjectHeap.POINTER_SIZE);
                                if (ptrNext < 0 || ptrNext >= m_memory.m_baseFreeBlockList) {
                                    block.m_error = FreeBlockListElement.ERROR.INVALID_POINTERS;
                                    block.m_errorText = "0x" + Long.toHexString(ptrNext);
                                }

                                // have block position before the marker byte for the walker
                                block.m_blockAddress = ptr - 1;
                                if (ptrPrev == SmallObjectHeap.INVALID_ADDRESS) {
                                    block.m_prevBlockAddress = -1;
                                } else {
                                    block.m_prevBlockAddress = ptrPrev;
                                }
                                if (ptrNext == SmallObjectHeap.INVALID_ADDRESS) {
                                    block.m_nextBlockAddress = -1;
                                } else {
                                    block.m_nextBlockAddress = ptrNext;
                                }

                                ptr = ptrNext;

                                break;
                            }

                            default: {
                                block.m_error = FreeBlockListElement.ERROR.INVALID_MARKER_BYTE;
                                block.m_errorText = "0x" + Integer.toHexString(marker);
                                break;
                            }
                        }

                        if (list != null) {
                            list.m_blocks.add(block);
                        }

                        if (block.isCorrupted()) {
                            if (list != null) {
                                list.m_isCorrupted = true;
                            }

                            System.out.println(block);
                        }

                    } while (ptr != SmallObjectHeap.INVALID_ADDRESS);
                }

                baseAddress += SmallObjectHeap.POINTER_SIZE;
            }

            if (p_heap != null) {
                if (list.isCorrupted()) {
                    p_heap.m_isCorrupted = true;
                }
            }
        }
    }

    /**
     * Detailed analysis data about the complete heap
     */
    public static class Heap {
        private long m_totalSize = -1;
        private boolean m_isCorrupted = false;

        private Map<Long, MemoryBlock> m_memoryBlocks = new HashMap<>();
        private List<FreeBlockList> m_freeBlockLists = new ArrayList<>();

        /**
         * Constructor
         */
        public Heap() {

        }

        /**
         * Get the total size of the heap
         *
         * @return Total size in byte
         */
        public long getTotalSize() {
            return m_totalSize;
        }

        /**
         * Check if the heap is corrupted (at least one analysied element is corrupted)
         *
         * @return True if corrupted, false if ok.
         */
        public boolean isCorrupted() {
            return m_isCorrupted;
        }

        /**
         * Get the list of memory blocks (allocated and free)
         *
         * @return List of memory blocks with addresses
         */
        public Map<Long, MemoryBlock> getMemoryBlocks() {
            return m_memoryBlocks;
        }

        /**
         * Get the doubly linked lists of free memory blocks
         *
         * @return List of double linked lists of free memory blocks
         */
        public List<FreeBlockList> getFreeBlockLists() {
            return m_freeBlockLists;
        }

        @Override
        public String toString() {
            StringBuilder output;
            output = new StringBuilder();

            if (m_isCorrupted) {
                output.append("!!! CORRUPTED !!! ");
            }
            output.append("Heap(m_totalSize ").append(m_totalSize).append("):\n");
            output.append("\n----- Memory blocks -----");

            Iterator<Entry<Long, MemoryBlock>> it;
            it = m_memoryBlocks.entrySet().iterator();
            while (it.hasNext()) {
                Entry<Long, MemoryBlock> entry;
                entry = it.next();

                output.append('\n');
                output.append(entry.getValue());
            }

            output.append("\n\n----- Free blocks lists (").append(m_freeBlockLists.size()).append(")-----");

            Iterator<FreeBlockList> it2;
            it2 = m_freeBlockLists.iterator();
            while (it2.hasNext()) {
                FreeBlockList list;
                list = it2.next();

                output.append('\n');
                output.append(list);
            }

            return output.toString();
        }
    }

    /**
     * Data about a single block of memory with the memory area of a segment.
     */
    public static final class MemoryBlock {
        enum ERROR {
            OK, INVALID_MARKER_BYTE, MARKER_BYTES_NOT_MATCHING, INVALID_LENGTH_FIELD_CONTENTS, INVALID_POINTERS
        }

        private long m_startAddress = -1;
        // end address excluding
        private long m_endAddress = -1;
        // markers don't count
        private long m_rawBlockSize = -1;
        private int m_markerByte = -1;
        private long m_blockPayloadSize = -1;
        private long m_nextFreeBlock = -1;
        private long m_prevFreeBlock = -1;
        private ERROR m_error = ERROR.OK;
        private String m_errorText = "";

        /**
         * Constructor
         */
        private MemoryBlock() {

        }

        /**
         * Start address of the memory block
         *
         * @return Address
         */
        public long getStartAddress() {
            return m_startAddress;
        }

        /**
         * End address of the memory block (excluding)
         *
         * @return Address
         */
        public long getEndAddress() {
            return m_endAddress;
        }

        /**
         * Full size of raw block
         *
         * @return Size of block in bytes
         */
        public long getRawBlockSize() {
            return m_rawBlockSize;
        }

        /**
         * Marker byte
         *
         * @return Marker byte
         */
        public int getMarkerByte() {
            return m_markerByte;
        }

        /**
         * Get the actual size available for a payload (available on alloc'd block, only)
         *
         * @return Size available for payload
         */
        public long getBlockPayloadSize() {
            return m_blockPayloadSize;
        }

        /**
         * Get the pointer of the next block of the double linked list of a free block (available on free blocks, only)
         *
         * @return Ptr
         */
        public long getNextFreeBlockAddress() {
            return m_nextFreeBlock;
        }

        /**
         * Get the pointer of the previous block of the double linked list of a free block (available on free blocks, only)
         *
         * @return Ptr
         */
        public long getPrevFreeBlockAddress() {
            return m_prevFreeBlock;
        }

        /**
         * Check if any errors were detected on this block
         *
         * @return Detected error
         */
        public ERROR getError() {
            return m_error;
        }

        /**
         * Get additional text for the flagged error
         *
         * @return Text for flagged error
         */
        public String getErrorText() {
            return m_errorText;
        }

        /**
         * Check if the block is corrupted
         *
         * @return True if corrupted, false otherwise
         */
        public boolean isCorrupted() {
            return m_error != ERROR.OK;
        }

        /**
         * Check if this is a free block
         *
         * @return True if free block, false otherwise
         */
        public boolean isFreeBlock() {
            return m_blockPayloadSize == -1;
        }

        /**
         * Check if this is a alloc'd block
         *
         * @return True if alloc'd block, false otherwise
         */
        public boolean isAllocatedBlock() {
            return m_blockPayloadSize != -1;
        }

        @Override
        public String toString() {
            String strBad = "";
            if (isCorrupted()) {
                strBad = "!!! CORRUPTED: " + m_error + ": " + m_errorText + " !!! ";
            }

            if (m_blockPayloadSize == -1) {
                return strBad + "FreeBlock [m_startAddress 0x" + Long.toHexString(m_startAddress) + ", m_endAddress 0x" + Long.toHexString(m_endAddress) +
                    ", m_rawBlockSize " + m_rawBlockSize + ", m_markerByte " + m_markerByte + ", m_prevFreeBlock 0x" + Long.toHexString(m_prevFreeBlock) +
                    ", m_nextFreeBlock 0x" + Long.toHexString(m_nextFreeBlock) + ']';
            } else {
                return strBad + "AllocatedBlock [m_startAddress 0x" + Long.toHexString(m_startAddress) + ", m_endAddress 0x" + Long.toHexString(m_endAddress) +
                    ", m_rawBlockSize " + m_rawBlockSize + ", m_markerByte " + m_markerByte + ", m_blockPayloadSize " + m_blockPayloadSize + ']';
            }
        }
    }

    /**
     * Data about a single free block within the free block list area of a segment.
     */
    private static class FreeBlockListElement {
        enum ERROR {
            OK, INVALID_BLOCK_SIZE, INVALID_POINTERS, INVALID_MARKER_BYTE
        }

        private long m_blockAddress = -1;
        private long m_prevBlockAddress = -1;
        private long m_nextBlockAddress = -1;
        private ERROR m_error = ERROR.OK;
        private String m_errorText = "";

        /**
         * Constructor
         */
        public FreeBlockListElement() {
        }

        /**
         * Get the address of the free block
         *
         * @return Address
         */
        public long getBlockAddress() {
            return m_blockAddress;
        }

        /**
         * Get the pointer of the previous block of the double linked list of a free block (available on free blocks, only)
         *
         * @return Ptr
         */
        public long getPrevBlockAddress() {
            return m_prevBlockAddress;
        }

        /**
         * Get the pointer of the next block of the double linked list of a free block (available on free blocks, only)
         *
         * @return Ptr
         */
        public long getNextBlockAddress() {
            return m_nextBlockAddress;
        }

        /**
         * Check if any errors were detected on this block
         *
         * @return Detected error
         */
        public ERROR getError() {
            return m_error;
        }

        /**
         * Get additional text for the flagged error
         *
         * @return Text for flagged error
         */
        public String getErrorText() {
            return m_errorText;
        }

        /**
         * Check if the block is corrupted
         *
         * @return True if corrupted, false otherwise
         */
        public boolean isCorrupted() {
            return m_error != ERROR.OK;
        }

        @Override
        public String toString() {
            String strBad = "";
            if (isCorrupted()) {
                strBad = "!!! CORRUPTED: " + m_error + ": " + m_errorText + " !!! ";
            }

            return strBad + "FreeBlockListElement[m_blockAddress 0x" + Long.toHexString(m_blockAddress) + ", m_prevBlockAddress 0x" +
                Long.toHexString(m_prevBlockAddress) + ", m_nextBlockAddress 0x" + Long.toHexString(m_nextBlockAddress) + ']';
        }
    }

    /**
     * Data about the free block list area within a segment.
     */
    private static class FreeBlockList {
        private long m_minFreeBlockSize = -1;
        private long m_addressRoot = -1;
        private List<FreeBlockListElement> m_blocks = new ArrayList<>();
        private boolean m_isCorrupted = false;

        /**
         * Constructor
         */
        public FreeBlockList() {
        }

        /**
         * Get the minimum size for the free blocks in the list
         *
         * @return Minimum block size
         */
        public long getMinFreeBlockSize() {
            return m_minFreeBlockSize;
        }

        /**
         * Get the address of the root of the doubly linked free blocks list
         *
         * @return Address
         */
        public long getAddressRoot() {
            return m_addressRoot;
        }

        /**
         * Get the free blocks list
         *
         * @return Free blocks list
         */
        public List<FreeBlockListElement> getBlocks() {
            return m_blocks;
        }

        /**
         * Check if at least one block if the block list is corrupted
         *
         * @return True if at least one block is corrupted, false if all blocks are ok
         */
        public boolean isCorrupted() {
            return m_isCorrupted;
        }

        @Override
        public String toString() {
            String strBad = "";
            if (m_isCorrupted) {
                strBad = "!!! CORRUPTED !!! ";
            }

            String str = strBad + "FreeBlockList[m_minFreeBlockSize " + m_minFreeBlockSize + ", m_addressRoot 0x" + Long.toHexString(m_addressRoot) + "]:";

            Iterator<FreeBlockListElement> it;
            it = m_blocks.iterator();
            while (it.hasNext()) {
                FreeBlockListElement block;
                block = it.next();

                str += "\n" + block;
            }

            return str;
        }
    }
}
