
package de.hhu.bsinfo.soh;

/**
 * The memory is divided into segments to enable better concurrent access
 * to the memory as a a whole. This also enables locking single segments
 * for house keeping tasks such as defragmentation.
 *
 * @author Florian Klein 04.04.2014
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.11.2015
 */
public final class SmallObjectHeapSegment {

	// Constants
	static final byte POINTER_SIZE = 5;
	private static final byte SMALL_BLOCK_SIZE = 64;
	private static final byte OCCUPIED_FLAGS_OFFSET = 0x5;
	private static final byte OCCUPIED_FLAGS_OFFSET_MASK = 0x03;
	private static final byte SINGLE_BYTE_MARKER = 0xF;

	// limits a single payload block to 8MB, though the allocator supports chaining of blocks
	// private static final int MAX_LENGTH_FIELD = 3;
	private static final int CHAINED_BLOCK_LENGTH_FIELD = POINTER_SIZE;
	public static final int MAX_SIZE_MEMORY_BLOCK = (int) Math.pow(2, 23);
	static final int SIZE_MARKER_BYTE = 1;

	// Attributes, have them accessible by the package to enable walking and analyzing the segment
	// don't modify or access them otherwise
	Storage m_memory;
	int m_segmentID;
	long m_base;
	long m_baseFreeBlockList;
	Status m_status;
	long m_assignedThread;

	// JNIReadWriteSpinLock m_lock;

	long[] m_freeBlockListSizes;

	long m_size = -1;
	long m_fullSize = -1;
	int m_freeBlocksListCount = -1;
	int m_freeBlocksListSizePerSegment = -1;
	long m_sizePayloadAllocated = -1;
	long m_numActiveMemoryBlocks = -1;

	// Constructors

	/**
	 * Creates an instance of Segment
	 *
	 * @param p_memory    The underlying storage to use for this segment.
	 * @param p_segmentID The ID of the segment
	 * @param p_base      The base address of the segment
	 * @param p_size      The size of the segment in bytes.
	 */
	public SmallObjectHeapSegment(final Storage p_memory, final int p_segmentID, final long p_base, final long p_size) {
		m_memory = p_memory;
		m_segmentID = p_segmentID;
		m_assignedThread = 0;
		m_base = p_base;
		m_fullSize = p_size;
		m_sizePayloadAllocated = 0;

		// m_lock = new JNIReadWriteSpinLock();

		// according to segment size, have a proper amount of
		// free memory block lists
		// -2, because we don't need a free block list for the full segment
		// and the first size greater than the full segment size
		// detect highest bit using log2 to have proper segment sizes
		m_freeBlocksListCount = (int) (Math.log(p_size) / Math.log(2)) - 2;
		m_freeBlocksListSizePerSegment = m_freeBlocksListCount * POINTER_SIZE;
		m_size = m_fullSize - m_freeBlocksListSizePerSegment;
		m_baseFreeBlockList = m_base + m_size;

		// Initializes the list sizes
		m_freeBlockListSizes = new long[m_freeBlocksListCount];
		for (int i = 0; i < m_freeBlocksListCount; i++) {
			m_freeBlockListSizes[i] = (long) Math.pow(2, i + 2);
		}
		m_freeBlockListSizes[0] = 12;
		m_freeBlockListSizes[1] = 24;
		m_freeBlockListSizes[2] = 36;
		m_freeBlockListSizes[3] = 48;

		// Create a free block in the complete memory
		// -2 for the marker bytes
		createFreeBlock(p_base + SIZE_MARKER_BYTE, m_size - SIZE_MARKER_BYTE * 2);
		m_status = new Status(m_size - SIZE_MARKER_BYTE * 2);
	}

	/**
	 * Get the total size of the segment in bytes.
	 *
	 * @return Size in bytes.
	 */
	public long getSize() {
		return m_fullSize;
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
	 * Get the amount of space allocated for actual data/payload.
	 *
	 * @return Amount of space in bytes allocated for actual payload.
	 */
	public long getSizeAllocatedPayload() {
		return m_sizePayloadAllocated;
	}

	/**
	 * Get the number of active memory blocks (allocated blocks).
	 *
	 * @return Number of active memory blocks.
	 */
	public long getNumActiveMemoryBlocks() {
		return m_numActiveMemoryBlocks;
	}

	/**
	 * Checks if the Segment is assigned to a thread.
	 *
	 * @return true if the Segment is assigned, false otherwise
	 */
	public boolean isAssigned() {
		return m_assignedThread != 0;
	}

	/**
	 * Assigns the Segment to a thread.
	 *
	 * @param p_threadID The thread ID to assign this segment to.
	 * @return the previously assigned thread
	 */
	public long assign(final long p_threadID) {
		long ret;

		ret = m_assignedThread;
		m_assignedThread = p_threadID;

		return ret;
	}

	/**
	 * Gets the current fragmentation in percentage
	 *
	 * @return the fragmentation
	 */
	public double getFragmentation() {
		double ret = 0;
		int free;
		int small;

		free = m_status.getFreeBlocks();
		small = m_status.getSmallBlocks();

		if (small >= 1 || free >= 1) {
			ret = (double) small / free;
		}

		return ret;
	}

	/**
	 * Allocate a memory block
	 *
	 * @param p_size the size of the block in bytes.
	 * @return the address of the block
	 */
	public long malloc(final int p_size) {

		if (p_size > MAX_SIZE_MEMORY_BLOCK) {
			// avoid huge allocations if not enough space
			if (p_size > m_status.getFreeSpace()) {
				return -1;
			}

			// slice into multiple blocks and chain them

			int slices = p_size / MAX_SIZE_MEMORY_BLOCK;
			if (p_size % MAX_SIZE_MEMORY_BLOCK > 0) {
				++slices;
			}

			// allocate blocks
			long[] blocks = new long[slices];
			for (int i = 0; i < blocks.length; i++) {
				if (i + 1 == blocks.length) {
					// last block
					blocks[i] = reserveBlock(p_size % MAX_SIZE_MEMORY_BLOCK, false);
				} else {
					blocks[i] = reserveBlock(MAX_SIZE_MEMORY_BLOCK, true);
				}
			}

			// check if allocation(s) failed (not enough space)
			for (int i = 0; i < blocks.length; i++) {
				if (blocks[i] == -1) {
					// roll back
					for (i = 0; i < blocks.length; i++) {
						if (blocks[i] != -1) {
							free(blocks[i]);
						}
					}

					return -1;
				}
			}

			// chain blocks from back to front, omit last block
			for (int i = 0; i < blocks.length - 1; i++) {
				writePointer(blocks[i], blocks[i + 1]);
			}

			// root of chain
			return blocks[0];
		} else {
			return reserveBlock(p_size, false);
		}
	}

	/**
	 * Frees a memory block
	 *
	 * @param p_address the address of the block
	 */
	public void free(final long p_address) {
		long address;
		int lengthFieldSize;

		address = p_address;
		while (true) {
			lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));

			// further blocks chained block
			if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD) {

				// read address of next block, free current
				long addrNext = readPointer(address);
				// blockSize var is a ptr here
				freeReservedBlock(address, lengthFieldSize, MAX_SIZE_MEMORY_BLOCK);
				address = addrNext;

			} else {
				freeReservedBlock(address, lengthFieldSize, getSizeMemoryBlock(address));
				break;
			}

		}
	}

	/**
	 * Get the size of an allocated block of memory.
	 *
	 * @param p_address Address of the block.
	 * @return Size of the block in bytes (payload only).
	 */
	public int getSizeBlock(final long p_address) {
		int lengthFieldSize;
		int size = 0;
		long address;

		assert assertSegmentBounds(p_address);

		address = p_address;
		while (true) {
			// skip length byte(s)
			lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));
			if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD) {
				// ptr to next chained block
				size += MAX_SIZE_MEMORY_BLOCK;
				address = read(address, lengthFieldSize);
			} else {
				size += (int) read(address, lengthFieldSize);
				break;
			}
		}

		return size;
	}

	/**
	 * Overwrites the bytes in the memory with the given value
	 *
	 * @param p_address the address to start
	 * @param p_size    the number of bytes to overwrite
	 * @param p_value   the value to write
	 */
	public void set(final long p_address, final long p_size, final byte p_value) {
		assert assertSegmentBounds(p_address, p_size);

		int lengthFieldSize;
		long address;
		long size;

		size = p_size;
		address = p_address;
		while (size > 0) {
			// skip length byte(s)
			lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));
			if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD) {
				if (size > MAX_SIZE_MEMORY_BLOCK) {
					m_memory.set(address + lengthFieldSize, MAX_SIZE_MEMORY_BLOCK, p_value);
					size -= MAX_SIZE_MEMORY_BLOCK;
				} else {
					m_memory.set(address + lengthFieldSize, size, p_value);
					size = 0;
				}

				address = readPointer(address);
			} else {
				m_memory.set(address + lengthFieldSize, size, p_value);
				break;
			}
		}

	}

	/**
	 * Read a single byte from the specified address + offset.
	 *
	 * @param p_address Address.
	 * @param p_offset  Offset to add to the address.
	 * @return Byte read.
	 */
	public byte readByte(final long p_address, final long p_offset) {
		assert assertSegmentBounds(p_address, p_offset);

		int lengthFieldSize;
		long size;
		long address;

		// skip length byte(s)
		address = p_address;
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));
		size = read(address, lengthFieldSize);

		if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD) {
			// determine which block the offset is in
			int blockCnt = (int) (p_offset / MAX_SIZE_MEMORY_BLOCK);

			// seek to block
			for (int i = 0; i < blockCnt; i++) {
				address = readPointer(address);
			}

			lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));
		} else {
			if (!(p_offset < size) || !(size >= Byte.BYTES && p_offset + Byte.BYTES <= size)) {
				return 0;
			}
		}

		return m_memory.readByte(address + lengthFieldSize + p_offset);
	}

	/**
	 * Read a single short from the specified address + offset.
	 *
	 * @param p_address Address.
	 * @param p_offset  Offset to add to the address.
	 * @return Short read.
	 */
	public short readShort(final long p_address, final long p_offset) {
		assert assertSegmentBounds(p_address, p_offset);

		int lengthFieldSize;
		int size;

		// skip length byte(s)
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
		size = (int) read(p_address, lengthFieldSize);

		if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD) {
			return (short) readChainedBlockValue(p_address, p_offset, Short.BYTES);
		} else {
			if (!(p_offset < size) || !(size >= Short.BYTES && p_offset + Short.BYTES <= size)) {
				return 0;
			}

			return m_memory.readShort(p_address + lengthFieldSize + p_offset);
		}
	}

	/**
	 * Read a single int from the specified address + offset.
	 *
	 * @param p_address Address.
	 * @param p_offset  Offset to add to the address.
	 * @return Int read.
	 */
	public int readInt(final long p_address, final long p_offset) {
		assert assertSegmentBounds(p_address, p_offset);

		int lengthFieldSize;
		int size;

		// skip length byte(s)
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
		size = (int) read(p_address, lengthFieldSize);

		if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD) {
			return (int) readChainedBlockValue(p_address, p_offset, Integer.BYTES);
		} else {
			if (!(p_offset < size) || !(size >= Integer.BYTES && p_offset + Integer.BYTES <= size)) {
				return 0;
			}

			return m_memory.readInt(p_address + lengthFieldSize + p_offset);
		}
	}

	/**
	 * Read a long from the specified address + offset.
	 *
	 * @param p_address Address.
	 * @param p_offset  Offset to add to the address.
	 * @return Long read.
	 */
	public long readLong(final long p_address, final long p_offset) {
		assert assertSegmentBounds(p_address, p_offset);

		int lengthFieldSize;
		int size;

		// skip length byte(s)
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
		size = (int) read(p_address, lengthFieldSize);

		if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD) {
			return (long) readChainedBlockValue(p_address, p_offset, Long.BYTES);
		} else {
			if (!(p_offset < size) || !(size >= Long.BYTES || p_offset + Long.BYTES <= size)) {
				return 0;
			}

			return m_memory.readLong(p_address + lengthFieldSize + p_offset);
		}
	}

	/**
	 * Read data into a byte array.
	 *
	 * @param p_address     Address in heap to start at.
	 * @param p_offset      Offset to add to start address.
	 * @param p_buffer      Buffer to read into.
	 * @param p_offsetArray Offset within the buffer.
	 * @param p_length      Number of elements to read.
	 * @return Number of elements read.
	 */
	public int readBytes(final long p_address, final long p_offset, final byte[] p_buffer, final int p_offsetArray,
			final int p_length) {
		assert assertSegmentBounds(p_address, p_offset);

		int bytesRead = 0;
		int lengthFieldSize;
		long address;

		address = p_address;
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));

		if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD) {
			// determine which block the offset is in
			int blockCnt = (int) (p_offset / MAX_SIZE_MEMORY_BLOCK);
			long offset = p_offset;
			int offsetArray = p_offsetArray;

			// seek to block
			for (int i = 0; i < blockCnt; i++) {
				address = readPointer(address);
				offset -= MAX_SIZE_MEMORY_BLOCK;
			}

			// read
			long length = p_length;
			while (true) {
				lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));
				if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD
						&& (offset % MAX_SIZE_MEMORY_BLOCK) + length > MAX_SIZE_MEMORY_BLOCK) {
					// cur block and more
					int curSize = (int) (MAX_SIZE_MEMORY_BLOCK - offset);

					m_memory.readBytes(address + lengthFieldSize + offset, p_buffer, offsetArray, curSize);

					offsetArray += curSize;
					bytesRead += curSize;
					offset = 0;
					length -= curSize;

					// move on
					address = readPointer(address);
				} else {
					// last block, write what's left
					assert length <= Math.pow(2, 8 * lengthFieldSize);

					m_memory.readBytes(address + lengthFieldSize + offset, p_buffer, offsetArray, (int) length);

					bytesRead += length;
					break;
				}
			}

		} else {
			bytesRead = m_memory.readBytes(address + lengthFieldSize + p_offset, p_buffer, p_offsetArray, p_length);
		}

		return bytesRead;
	}

	/**
	 * Read data into a short array.
	 *
	 * @param p_address     Address in heap to start at.
	 * @param p_offset      Offset to add to start address.
	 * @param p_buffer      Buffer to read into.
	 * @param p_offsetArray Offset within the buffer.
	 * @param p_length      Number of elements to read.
	 * @return Number of elements read.
	 */
	public int readShorts(final long p_address, final long p_offset, final short[] p_buffer, final int p_offsetArray,
			final int p_length) {
		assert assertSegmentBounds(p_address, p_offset);

		int itemsRead = 0;
		int lengthFieldSize;
		long address;

		address = p_address;
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));
		if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD) {
			// determine which block the offset is in
			int blockCnt = (int) (p_offset / MAX_SIZE_MEMORY_BLOCK);
			long offset = p_offset;
			int offsetArray = p_offsetArray;
			// if a single short is split between two blocks, other shorts are split as well between further blocks
			boolean dataOnBlockSplit = p_offset % Short.BYTES > 0;

			// seek to block
			for (int i = 0; i < blockCnt; i++) {
				address = readPointer(address);
				offset -= MAX_SIZE_MEMORY_BLOCK;
			}

			// read
			long length = p_length;
			while (true) {
				lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));
				if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD
						&& (offset % MAX_SIZE_MEMORY_BLOCK) + length * Short.BYTES > MAX_SIZE_MEMORY_BLOCK) {
					// cur block and more
					int curItems = (int) (MAX_SIZE_MEMORY_BLOCK - offset) / Short.BYTES;

					readShorts(address + lengthFieldSize + offset, p_buffer, offsetArray, curItems);
					offsetArray += curItems;
					itemsRead += curItems;
					length -= curItems;

					// don't read last (and first) item of block normally
					if (dataOnBlockSplit) {
						offset += curItems * Short.BYTES;

						p_buffer[offsetArray] = (short) readChainedBlockValue(address, offset, Short.BYTES);
						offset = Short.BYTES - (MAX_SIZE_MEMORY_BLOCK - offset);
						++offsetArray;
						++itemsRead;
						--length;
					} else {
						offset = 0;
					}

					// move on
					address = readPointer(address);
				} else {
					// last block, read what's left
					assert length * Short.BYTES <= Math.pow(2, 8 * lengthFieldSize);

					readShorts(address + lengthFieldSize + offset, p_buffer, offsetArray, (int) length);

					itemsRead += length;
					break;
				}
			}
		} else {
			itemsRead = readShorts(address + lengthFieldSize + p_offset, p_buffer, p_offsetArray, p_length);
		}

		return itemsRead;
	}

	/**
	 * Read data into an int array.
	 *
	 * @param p_address     Address in heap to start at.
	 * @param p_offset      Offset to add to start address.
	 * @param p_buffer      Buffer to read into.
	 * @param p_offsetArray Offset within the buffer.
	 * @param p_length      Number of elements to read.
	 * @return Number of elements read.
	 */
	public int readInts(final long p_address, final long p_offset, final int[] p_buffer, final int p_offsetArray,
			final int p_length) {
		assert assertSegmentBounds(p_address, p_offset);

		int itemsRead = 0;
		int lengthFieldSize;
		long address;

		address = p_address;
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));

		if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD) {
			// determine which block the offset is in
			int blockCnt = (int) (p_offset / MAX_SIZE_MEMORY_BLOCK);
			long offset = p_offset;
			int offsetArray = p_offsetArray;
			// if a single short is split between two blocks, other shorts are split as well between further blocks
			boolean dataOnBlockSplit = p_offset % Integer.BYTES > 0;

			// seek to block
			for (int i = 0; i < blockCnt; i++) {
				address = readPointer(address);
				offset -= MAX_SIZE_MEMORY_BLOCK;
			}

			// read
			long length = p_length;
			while (true) {
				lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));
				if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD
						&& (offset % MAX_SIZE_MEMORY_BLOCK) + length * Integer.BYTES > MAX_SIZE_MEMORY_BLOCK) {
					// cur block and more
					int curItems = (int) (MAX_SIZE_MEMORY_BLOCK - offset) / Integer.BYTES;

					readInts(address + lengthFieldSize + offset, p_buffer, offsetArray, curItems);
					offsetArray += curItems;
					itemsRead += curItems;
					length -= curItems;

					// don't read last (and first) item of block normally
					if (dataOnBlockSplit) {
						offset += curItems * Integer.BYTES;

						p_buffer[offsetArray] = (int) readChainedBlockValue(address, offset, Integer.BYTES);
						offset = Integer.BYTES - (MAX_SIZE_MEMORY_BLOCK - offset);
						++offsetArray;
						++itemsRead;
						--length;
					} else {
						offset = 0;
					}

					// move on
					address = readPointer(address);
				} else {
					// last block, read what's left
					assert length * Integer.BYTES <= Math.pow(2, 8 * lengthFieldSize);

					readInts(address + lengthFieldSize + offset, p_buffer, offsetArray, (int) length);

					itemsRead += length;
					break;
				}
			}
		} else {
			itemsRead = readInts(address + lengthFieldSize + p_offset, p_buffer, p_offsetArray, p_length);
		}

		return itemsRead;
	}

	/**
	 * Read data into a long array.
	 *
	 * @param p_address     Address in heap to start at.
	 * @param p_offset      Offset to add to start address.
	 * @param p_buffer      Buffer to read into.
	 * @param p_offsetArray Offset within the buffer.
	 * @param p_length      Number of elements to read.
	 * @return Number of elements read.
	 */
	public int readLongs(final long p_address, final long p_offset, final long[] p_buffer, final int p_offsetArray,
			final int p_length) {
		assert assertSegmentBounds(p_address, p_offset);

		int itemsRead = 0;
		int lengthFieldSize;
		long address;

		address = p_address;
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));

		if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD) {
			// determine which block the offset is in
			int blockCnt = (int) (p_offset / MAX_SIZE_MEMORY_BLOCK);
			long offset = p_offset;
			int offsetArray = p_offsetArray;
			// if a single short is split between two blocks, other shorts are split as well between further blocks
			boolean dataOnBlockSplit = p_offset % Long.BYTES > 0;

			// seek to block
			for (int i = 0; i < blockCnt; i++) {
				address = readPointer(address);
				offset -= MAX_SIZE_MEMORY_BLOCK;
			}

			// read
			long length = p_length;
			while (true) {
				lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));
				if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD
						&& (offset % MAX_SIZE_MEMORY_BLOCK) + length * Long.BYTES > MAX_SIZE_MEMORY_BLOCK) {
					// cur block and more
					int curItems = (int) (MAX_SIZE_MEMORY_BLOCK - offset) / Long.BYTES;

					readLongs(address + lengthFieldSize + offset, p_buffer, offsetArray, curItems);
					offsetArray += curItems;
					itemsRead += curItems;
					length -= curItems;

					// don't read last (and first) item of block normally
					if (dataOnBlockSplit) {
						offset += curItems * Long.BYTES;

						p_buffer[offsetArray] = readChainedBlockValue(address, offset, Long.BYTES);
						offset = Long.BYTES - (MAX_SIZE_MEMORY_BLOCK - offset);
						++offsetArray;
						++itemsRead;
						--length;
					} else {
						offset = 0;
					}

					// move on
					address = readPointer(address);
				} else {
					// last block, read what's left
					assert length * Long.BYTES <= Math.pow(2, 8 * lengthFieldSize);

					readLongs(address + lengthFieldSize + offset, p_buffer, offsetArray, (int) length);

					itemsRead += length;
					break;
				}
			}
		} else {
			itemsRead = readLongs(address + lengthFieldSize + p_offset, p_buffer, p_offsetArray, p_length);
		}

		return itemsRead;
	}

	/**
	 * Write a single byte to the specified address + offset.
	 *
	 * @param p_address Address.
	 * @param p_offset  Offset to add to the address.
	 * @param p_value   Byte to write.
	 */
	public void writeByte(final long p_address, final long p_offset, final byte p_value) {
		assert assertSegmentBounds(p_address, p_offset);

		int lengthFieldSize;
		long address;

		// skip length byte(s)
		address = p_address;
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));

		if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD) {
			// determine which block the offset is in
			int blockCnt = (int) (p_offset / MAX_SIZE_MEMORY_BLOCK);

			// seek to block
			for (int i = 0; i < blockCnt; i++) {
				address = readPointer(address);
			}

			lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));
		}

		m_memory.writeByte(address + lengthFieldSize + p_offset, p_value);
	}

	/**
	 * Write a short to the specified address + offset.
	 *
	 * @param p_address Address.
	 * @param p_offset  Offset to add to the address.
	 * @param p_value   Short to write.
	 */
	public void writeShort(final long p_address, final long p_offset, final short p_value) {
		assert assertSegmentBounds(p_address, p_offset);

		int lengthFieldSize;

		// skip length byte(s)
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

		if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD) {
			writeChainedBlockValue(p_address, p_offset, p_value, Short.BYTES);
		} else {
			m_memory.writeShort(p_address + lengthFieldSize + p_offset, p_value);
		}
	}

	/**
	 * Write a single int to the specified address + offset.
	 *
	 * @param p_address Address.
	 * @param p_offset  Offset to add to the address.
	 * @param p_value   int to write.
	 */
	public void writeInt(final long p_address, final long p_offset, final int p_value) {
		assert assertSegmentBounds(p_address, p_offset);

		int lengthFieldSize;

		// skip length byte(s)
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

		if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD) {
			writeChainedBlockValue(p_address, p_offset, p_value, Integer.BYTES);
		} else {
			m_memory.writeInt(p_address + lengthFieldSize + p_offset, p_value);
		}
	}

	/**
	 * Write a long value to the specified address + offset.
	 *
	 * @param p_address Address.
	 * @param p_offset  Offset to add to the address.
	 * @param p_value   Long value to write.
	 */
	public void writeLong(final long p_address, final long p_offset, final long p_value) {
		assert assertSegmentBounds(p_address, p_offset);

		int lengthFieldSize;

		// skip length byte(s)
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

		if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD) {
			writeChainedBlockValue(p_address, p_offset, p_value, Long.BYTES);
		} else {
			m_memory.writeLong(p_address + lengthFieldSize + p_offset, p_value);
		}
	}

	/**
	 * Write an array of bytes to the specified address + offset.
	 *
	 * @param p_address     Address.
	 * @param p_offset      Offset to add to the address.
	 * @param p_value       Bytes to write.
	 * @param p_offsetArray Offset within the buffer.
	 * @param p_length      Number of elements to read.
	 * @return Number of elements written.
	 */
	public int writeBytes(final long p_address, final long p_offset, final byte[] p_value, final int p_offsetArray,
			final int p_length) {
		assert assertSegmentBounds(p_address, p_offset);

		int bytesWritten = 0;
		int lengthFieldSize;
		long address;

		address = p_address;
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));

		if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD) {
			// determine which block the offset is in
			int blockCnt = (int) (p_offset / MAX_SIZE_MEMORY_BLOCK);
			long offset = p_offset;
			int offsetArray = p_offsetArray;

			// seek to block
			for (int i = 0; i < blockCnt; i++) {
				address = readPointer(address);
				offset -= MAX_SIZE_MEMORY_BLOCK;
			}

			// read
			long length = p_length;
			while (true) {
				lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));
				if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD
						&& (offset % MAX_SIZE_MEMORY_BLOCK) + length > MAX_SIZE_MEMORY_BLOCK) {
					// cur block and more
					int curSize = (int) (MAX_SIZE_MEMORY_BLOCK - offset);

					m_memory.writeBytes(address + lengthFieldSize + offset, p_value, offsetArray, curSize);

					offsetArray += curSize;
					bytesWritten += curSize;
					offset = 0;
					length -= curSize;

					// move on
					address = readPointer(address);
				} else {
					// last block, write what's left
					assert length <= Math.pow(2, 8 * lengthFieldSize);

					m_memory.writeBytes(address + lengthFieldSize + offset, p_value, offsetArray, (int) length);

					bytesWritten += length;
					break;
				}
			}

		} else {
			bytesWritten = m_memory.writeBytes(address + lengthFieldSize + p_offset, p_value, p_offsetArray, p_length);
		}

		return bytesWritten;
	}

	/**
	 * Write an array of shorts to the heap.
	 *
	 * @param p_address     Address of an allocated block of memory.
	 * @param p_offset      Offset within the block of memory to start at.
	 * @param p_value       Array to write.
	 * @param p_offsetArray Offset within the array.
	 * @param p_length      Number of elements to write.
	 * @return Number of elements written.
	 */
	int writeShorts(final long p_address, final long p_offset, final short[] p_value, final int p_offsetArray,
			final int p_length) {
		assert assertSegmentBounds(p_address, p_offset);

		int itemsWritten = 0;
		int lengthFieldSize;
		long address;

		address = p_address;
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

		if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD) {
			// determine which block the offset is in
			int blockCnt = (int) (p_offset / MAX_SIZE_MEMORY_BLOCK);
			long offset = p_offset;
			int offsetArray = p_offsetArray;
			// if a single short is split between two blocks, other shorts are split as well between further blocks
			boolean dataOnBlockSplit = p_offset % Short.BYTES > 0;

			// seek to block
			for (int i = 0; i < blockCnt; i++) {
				address = readPointer(address);
				offset -= MAX_SIZE_MEMORY_BLOCK;
			}

			// write
			long length = p_length;
			while (true) {
				lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));
				if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD
						&& (offset % MAX_SIZE_MEMORY_BLOCK) + length * Short.BYTES > MAX_SIZE_MEMORY_BLOCK) {
					// cur block and more
					int curItems = (int) (MAX_SIZE_MEMORY_BLOCK - offset) / Short.BYTES;

					writeShorts(address + lengthFieldSize + offset, p_value, offsetArray, curItems);
					offsetArray += curItems;
					itemsWritten += curItems;
					length -= curItems;

					// don't read last (and first) item of block normally
					if (dataOnBlockSplit) {
						offset += curItems * Short.BYTES;

						writeChainedBlockValue(address, offset, p_value[offsetArray], Short.BYTES);
						offset = Short.BYTES - (MAX_SIZE_MEMORY_BLOCK - offset);
						++offsetArray;
						++itemsWritten;
						--length;
					} else {
						offset = 0;
					}

					// move on
					address = readPointer(address);
				} else {
					// last block, read what's left
					assert length * Short.BYTES <= Math.pow(2, 8 * lengthFieldSize);

					writeShorts(address + lengthFieldSize + offset, p_value, offsetArray, (int) length);

					itemsWritten += length;
					break;
				}
			}
		} else {
			itemsWritten = writeShorts(address + lengthFieldSize + p_offset, p_value, p_offsetArray, p_length);
		}

		return itemsWritten;
	}

	/**
	 * Write an array of ints to the heap.
	 *
	 * @param p_address     Address of an allocated block of memory.
	 * @param p_offset      Offset within the block of memory to start at.
	 * @param p_value       Array to write.
	 * @param p_offsetArray Offset within the array.
	 * @param p_length      Number of elements to write.
	 * @return Number of elements written.
	 */
	int writeInts(final long p_address, final long p_offset, final int[] p_value, final int p_offsetArray,
			final int p_length) {
		assert assertSegmentBounds(p_address, p_offset);

		int itemsWritten = 0;
		int lengthFieldSize;
		long address;

		address = p_address;
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));

		if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD) {
			// determine which block the offset is in
			int blockCnt = (int) (p_offset / MAX_SIZE_MEMORY_BLOCK);
			long offset = p_offset;
			int offsetArray = p_offsetArray;
			// if a single short is split between two blocks, other shorts are split as well between further blocks
			boolean dataOnBlockSplit = p_offset % Integer.BYTES > 0;

			// seek to block
			for (int i = 0; i < blockCnt; i++) {
				address = readPointer(address);
				offset -= MAX_SIZE_MEMORY_BLOCK;
			}

			// write
			long length = p_length;
			while (true) {
				lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));
				if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD
						&& (offset % MAX_SIZE_MEMORY_BLOCK) + length * Integer.BYTES > MAX_SIZE_MEMORY_BLOCK) {
					// cur block and more
					int curItems = (int) (MAX_SIZE_MEMORY_BLOCK - offset) / Integer.BYTES;

					writeInts(address + lengthFieldSize + offset, p_value, offsetArray, curItems);
					offsetArray += curItems;
					itemsWritten += curItems;
					length -= curItems;

					// don't read last (and first) item of block normally
					if (dataOnBlockSplit) {
						offset += curItems * Integer.BYTES;

						writeChainedBlockValue(address, offset, p_value[offsetArray], Integer.BYTES);
						offset = Integer.BYTES - (MAX_SIZE_MEMORY_BLOCK - offset);
						++offsetArray;
						++itemsWritten;
						--length;
					} else {
						offset = 0;
					}

					// move on
					address = readPointer(address);
				} else {
					// last block, read what's left
					assert length * Integer.BYTES <= Math.pow(2, 8 * lengthFieldSize);

					writeInts(address + lengthFieldSize + offset, p_value, offsetArray, (int) length);

					itemsWritten += length;
					break;
				}
			}
		} else {
			itemsWritten = writeInts(address + lengthFieldSize + p_offset, p_value, p_offsetArray, p_length);
		}

		return itemsWritten;
	}

	/**
	 * Write an array of longs to the heap.
	 *
	 * @param p_address     Address of an allocated block of memory.
	 * @param p_offset      Offset within the block of memory to start at.
	 * @param p_value       Array to write.
	 * @param p_offsetArray Offset within the array.
	 * @param p_length      Number of elements to write.
	 * @return Number of elements written.
	 */
	int writeLongs(final long p_address, final long p_offset, final long[] p_value, final int p_offsetArray,
			final int p_length) {
		assert assertSegmentBounds(p_address, p_offset);

		int itemsWritten = 0;
		int lengthFieldSize;
		long address;

		address = p_address;
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));

		if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD) {
			// determine which block the offset is in
			int blockCnt = (int) (p_offset / MAX_SIZE_MEMORY_BLOCK);
			long offset = p_offset;
			int offsetArray = p_offsetArray;
			// if a single short is split between two blocks, other shorts are split as well between further blocks
			boolean dataOnBlockSplit = p_offset % Long.BYTES > 0;

			// seek to block
			for (int i = 0; i < blockCnt; i++) {
				address = readPointer(address);
				offset -= MAX_SIZE_MEMORY_BLOCK;
			}

			// write
			long length = p_length;
			while (true) {
				lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));
				if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD
						&& (offset % MAX_SIZE_MEMORY_BLOCK) + length * Long.BYTES > MAX_SIZE_MEMORY_BLOCK) {
					// cur block and more
					int curItems = (int) (MAX_SIZE_MEMORY_BLOCK - offset) / Long.BYTES;

					writeLongs(address + lengthFieldSize + offset, p_value, offsetArray, curItems);
					offsetArray += curItems;
					itemsWritten += curItems;
					length -= curItems;

					// don't read last (and first) item of block normally
					if (dataOnBlockSplit) {
						offset += curItems * Long.BYTES;

						writeChainedBlockValue(address, offset, p_value[offsetArray], Long.BYTES);
						offset = Long.BYTES - (MAX_SIZE_MEMORY_BLOCK - offset);
						++offsetArray;
						++itemsWritten;
						--length;
					} else {
						offset = 0;
					}

					// move on
					address = readPointer(address);
				} else {
					// last block, read what's left
					assert length * Long.BYTES <= Math.pow(2, 8 * lengthFieldSize);

					writeLongs(address + lengthFieldSize + offset, p_value, offsetArray, (int) length);

					itemsWritten += length;
					break;
				}
			}
		} else {
			itemsWritten = writeLongs(address + lengthFieldSize + p_offset, p_value, p_offsetArray, p_length);
		}

		return itemsWritten;
	}

	@Override
	public String toString() {
		return "Segment(id: " + m_segmentID + ", m_assignedThread " + m_assignedThread + "): "
				+ "m_base " + m_base + ", m_baseFreeBlockList "
				+ m_baseFreeBlockList + ", m_size "
				+ m_size + ", m_fullSize "
				+ m_fullSize + ", segment borders: "
				+ m_base + "|" + (m_base + m_fullSize) + ", status: "
				+ m_status;
	}

	// -------------------------------------------------------------------------------------------

	/**
	 * Get the size of the allocated or free'd block of memory specified
	 * by the given address.
	 *
	 * @param p_address Address of block to get the size of.
	 * @return Size of memory block at specified address.
	 */
	private long getSizeMemoryBlock(final long p_address) {
		int lengthFieldSize;

		assert assertSegmentBounds(p_address);

		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
		return read(p_address, lengthFieldSize);
	}

	/**
	 * Reserve a free block of memory.
	 *
	 * @param p_size         Size of the block (payload size).
	 * @param p_chainedBlock True if this block is part of a chain of blocks and not the last block in the chain.
	 * @return Address of the reserved block or null if out of memory of no block for requested size was found.
	 */
	private long reserveBlock(final int p_size, final boolean p_chainedBlock) {
		long ret = -1;
		int list;
		long address;
		long blockSize;
		int lengthFieldSize;
		long freeSize;
		int freeLengthFieldSize;
		byte blockMarker;

		if (p_size > 0 && p_size <= MAX_SIZE_MEMORY_BLOCK) {
			if (!p_chainedBlock) {
				if (p_size >= 1 << 16) {
					lengthFieldSize = 3;
				} else if (p_size >= 1 << 8) {
					lengthFieldSize = 2;
				} else {
					lengthFieldSize = 1;
				}

				blockMarker = (byte) (OCCUPIED_FLAGS_OFFSET + lengthFieldSize);
			} else {
				// length field contains the ptr to the next block in the chain
				// payload block is still MAX_SIZE_MEMORY_BLOCK in size
				lengthFieldSize = CHAINED_BLOCK_LENGTH_FIELD;

				// use lengthFieldSize state to indicate block size POINTER_SIZE (5) and chained block
				blockMarker = (byte) (OCCUPIED_FLAGS_OFFSET + lengthFieldSize);
			}

			blockSize = p_size + lengthFieldSize;

			// Get the list with a free block which is big enough
			list = getList(blockSize) + 1;
			while (list < m_freeBlocksListCount && readPointer(m_baseFreeBlockList + list * POINTER_SIZE) == 0) {
				list++;
			}
			if (list < m_freeBlocksListCount) {
				// A list is found
				address = readPointer(m_baseFreeBlockList + list * POINTER_SIZE);
			} else {
				// Traverse through the lower list
				list = getList(blockSize);
				address = readPointer(m_baseFreeBlockList + list * POINTER_SIZE);
				if (address != 0) {
					freeLengthFieldSize = readRightPartOfMarker(address - 1);
					freeSize = read(address, freeLengthFieldSize);
					while (freeSize < blockSize && address != 0) {
						address = readPointer(address + freeLengthFieldSize + POINTER_SIZE);
						if (address != 0) {
							freeLengthFieldSize = readRightPartOfMarker(address - 1);
							freeSize = read(address, freeLengthFieldSize);
						}
					}
				}
			}

			if (address != 0) {
				// Unhook the free block
				unhookFreeBlock(address);

				freeLengthFieldSize = readRightPartOfMarker(address - SIZE_MARKER_BYTE);
				freeSize = read(address, freeLengthFieldSize);
				if (freeSize == blockSize) {
					m_status.m_freeSpace -= blockSize;
					m_status.m_freeBlocks--;
					if (freeSize < SMALL_BLOCK_SIZE) {
						m_status.m_freeSmall64ByteBlocks--;
					}
				} else if (freeSize == blockSize + 1) {
					// 1 Byte to big -> write two markers on the right
					writeRightPartOfMarker(address + blockSize, SINGLE_BYTE_MARKER);
					writeLeftPartOfMarker(address + blockSize + 1, SINGLE_BYTE_MARKER);

					// +1 for the marker byte added
					m_status.m_freeSpace -= blockSize + 1;
					m_status.m_freeBlocks--;
					if (freeSize + 1 < SMALL_BLOCK_SIZE) {
						m_status.m_freeSmall64ByteBlocks--;
					}
				} else {
					// Block is too big -> create a new free block with the remaining size
					createFreeBlock(address + blockSize + 1, freeSize - blockSize - 1);

					// +1 for the marker byte added
					m_status.m_freeSpace -= blockSize + 1;

					if (freeSize >= SMALL_BLOCK_SIZE && freeSize - blockSize - 1 < SMALL_BLOCK_SIZE) {
						m_status.m_freeSmall64ByteBlocks++;
					}
				}
				// Write marker
				writeLeftPartOfMarker(address + blockSize, blockMarker);
				writeRightPartOfMarker(address - SIZE_MARKER_BYTE, blockMarker);

				// Write block size (or ptr to next block)
				if (lengthFieldSize == CHAINED_BLOCK_LENGTH_FIELD) {
					write(address, 0xFFFFFFFFFFL, lengthFieldSize);
				} else {
					write(address, p_size, lengthFieldSize);
				}

				ret = address;
			}
		}

		if (ret != -1) {
			m_sizePayloadAllocated += p_size;
			m_numActiveMemoryBlocks++;
		}

		return ret;
	}

	/**
	 * Free a reserved block of memory
	 *
	 * @param p_address         Address of the block
	 * @param p_lengthFieldSize Size of the length field
	 * @param p_blockSize       Size of the block's payload
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

		assert assertSegmentBounds(p_address);

		blockSize = p_blockSize;
		lengthFieldSize = p_lengthFieldSize;
		freeSize = blockSize + lengthFieldSize;
		address = p_address;

		// Only merge if left neighbor within same segment
		if (address - SIZE_MARKER_BYTE != m_base) {
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
			// Do not merge across segments
			leftSize = 0;
			leftFree = false;
		}

		// update start address of free block and size
		address -= leftSize;
		freeSize += leftSize;

		// Only merge if right neighbor within same segment, +1 for marker byte
		if (address + blockSize + SIZE_MARKER_BYTE != m_base + m_size) {
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
			// Do not merge across segments
			rightSize = 0;
			rightFree = false;
		}

		// update size of full free block
		freeSize += rightSize;

		// Create a free block
		createFreeBlock(address, freeSize);

		if (!leftFree && !rightFree) {
			m_status.m_freeSpace += blockSize + lengthFieldSize;
			m_status.m_freeBlocks++;
			if (blockSize + lengthFieldSize < SMALL_BLOCK_SIZE) {
				m_status.m_freeSmall64ByteBlocks++;
			}
		} else if (leftFree && !rightFree) {
			m_status.m_freeSpace += blockSize + lengthFieldSize + SIZE_MARKER_BYTE;
			if (blockSize + lengthFieldSize + leftSize >= SMALL_BLOCK_SIZE && leftSize < SMALL_BLOCK_SIZE) {
				m_status.m_freeSmall64ByteBlocks--;
			}
		} else if (!leftFree /*&& rightFree*/) {
			m_status.m_freeSpace += blockSize + lengthFieldSize + SIZE_MARKER_BYTE;
			if (blockSize + lengthFieldSize + rightSize >= SMALL_BLOCK_SIZE && rightSize < SMALL_BLOCK_SIZE) {
				m_status.m_freeSmall64ByteBlocks--;
			}
			// leftFree && rightFree
		} else {
			// +2 for two marker bytes being merged
			m_status.m_freeSpace += blockSize + lengthFieldSize + 2 * SIZE_MARKER_BYTE;
			m_status.m_freeBlocks--;
			if (blockSize + lengthFieldSize + leftSize + rightSize >= SMALL_BLOCK_SIZE) {
				if (rightSize < SMALL_BLOCK_SIZE && leftSize < SMALL_BLOCK_SIZE) {
					m_status.m_freeSmall64ByteBlocks--;
				} else if (rightSize >= SMALL_BLOCK_SIZE && leftSize >= SMALL_BLOCK_SIZE) {
					m_status.m_freeSmall64ByteBlocks++;
				}
			}
		}

		m_sizePayloadAllocated -= blockSize;
		m_numActiveMemoryBlocks--;
	}

	/**
	 * Read a value from a series of chained blocks.
	 * This handles a value split over two blocks as well.
	 *
	 * @param p_address   Address of the block (root of the chained blocks)
	 * @param p_offset    Offset within the whole block
	 * @param p_valLength Length of the value to read
	 * @return The read value (cast to an appropriate data type depending on specified length).
	 */
	private long readChainedBlockValue(final long p_address, final long p_offset, final int p_valLength) {
		long address;
		int lengthFieldSize;
		long offset;

		address = p_address;
		offset = p_offset;

		// determine which block the offset is in
		int blockCnt = (int) (p_offset / MAX_SIZE_MEMORY_BLOCK);

		// seek to block
		for (int i = 0; i < blockCnt; i++) {
			address = readPointer(address);
			offset -= MAX_SIZE_MEMORY_BLOCK;
		}

		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));

		// unfortunate: one part of the variable is in the current and the other one in
		// the next block
		int frag1Size = (int) (MAX_SIZE_MEMORY_BLOCK - offset);
		if (frag1Size > p_valLength) {
			frag1Size = p_valLength;
		}
		int frag2Size = p_valLength - frag1Size;

		if (frag2Size > 0) {
			long frag1 = read(address + lengthFieldSize + offset, frag1Size);

			// next block
			address = readPointer(address);
			lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));

			long frag2 = read(address + lengthFieldSize, frag2Size);

			// assemble, assuming little endian byte order
			return ((frag2 & (0xFFFFFFFFFFFFFFFFL >>> (64 - (frag2Size * 8)))) << (frag1Size * 8))
					| (frag1 & (0xFFFFFFFFFFFFFFFFL >>> (64 - (frag1Size * 8))));
		} else {
			switch (p_valLength) {
				case Short.BYTES:
					return m_memory.readShort(address + lengthFieldSize + offset);
				case Integer.BYTES:
					return m_memory.readInt(address + lengthFieldSize + offset);
				case Long.BYTES:
					return m_memory.readLong(address + lengthFieldSize + offset);
				default:
					return m_memory.readVal(address + lengthFieldSize + offset, p_valLength);
			}

		}
	}

	/**
	 * Write a value to a memory location consisting of a series of chained blocks.
	 * This handles a value split over two blocks as well.
	 *
	 * @param p_address   Address of the block (root of the chained blocks)
	 * @param p_offset    Offset within the whole block
	 * @param p_value     Value to write
	 * @param p_valLength Length of the value to read
	 */
	private void writeChainedBlockValue(final long p_address, final long p_offset, final long p_value,
			final int p_valLength) {
		long address;
		int lengthFieldSize;
		long offset;

		address = p_address;
		offset = p_offset;

		// determine which block the offset is in
		int blockCnt = (int) (p_offset / MAX_SIZE_MEMORY_BLOCK);

		// seek to block
		for (int i = 0; i < blockCnt; i++) {
			address = readPointer(address);
			offset -= MAX_SIZE_MEMORY_BLOCK;
		}

		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));

		// unfortunate: one part of the variable is in the current and the other one in
		// the next block
		int frag1Size = (int) (MAX_SIZE_MEMORY_BLOCK - offset);
		if (frag1Size > p_valLength) {
			frag1Size = p_valLength;
		}
		int frag2Size = p_valLength - frag1Size;

		if (frag2Size > 0) {
			// assuming little endian byte order
			long frag1 = p_value & (0xFFFFFFFFFFFFFFFFL >>> (64 - (frag1Size * 8)));
			long frag2 = (p_value >> (frag1Size * 8)) & (0xFFFFFFFFFFFFFFFFL >>> (64 - (frag2Size * 8)));

			// fragment 1 first (little endian)
			write(address + lengthFieldSize + offset, frag1, frag1Size);

			// next block
			address = readPointer(address);
			lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - SIZE_MARKER_BYTE));

			write(address + lengthFieldSize, frag2, frag2Size);
		} else {
			switch (p_valLength) {
				case Short.BYTES:
					m_memory.writeShort(address + lengthFieldSize + offset, (short) p_value);
					break;
				case Integer.BYTES:
					m_memory.writeInt(address + lengthFieldSize + offset, (int) p_value);
					break;
				case Long.BYTES:
					m_memory.writeLong(address + lengthFieldSize + offset, p_value);
					break;
				default:
					m_memory.writeVal(address + lengthFieldSize + offset, p_value, p_valLength);
					break;
			}

		}
	}

	/**
	 * Check the segment bounds with the specified address.
	 *
	 * @param p_address Address to check if within segment.
	 * @return Dummy return for assert
	 */
	private boolean assertSegmentBounds(final long p_address) {
		if (p_address < m_base || p_address > m_base + m_fullSize) {
			throw new MemoryRuntimeException("Address " + p_address + " is not within segment: " + this);
		}

		return true;
	}

	/**
	 * Check the segment bounds with the specified start address and size.
	 *
	 * @param p_address Address to check if within bounds.
	 * @param p_length  Number of bytes starting at address.
	 * @return Dummy return for assert
	 */
	private boolean assertSegmentBounds(final long p_address, final long p_length) {
		if (p_address < m_base || p_address > m_base + m_fullSize
				|| p_address + p_length < m_base || p_address + p_length > m_base + m_fullSize) {
			throw new MemoryRuntimeException("Address " + p_address
					+ " with length " + p_length + "is not within segment: " + this);
		}

		return true;
	}

	/**
	 * Check if the specified size is within the range of the max blocksize.
	 *
	 * @param p_size Size to check if not exceeding max blocksize.
	 * @return Dummy return for assert
	 */
	private boolean assertSegmentMaxBlocksize(final long p_size) {
		if (p_size > MAX_SIZE_MEMORY_BLOCK) {
			throw new MemoryRuntimeException("Size " + p_size
					+ " exceeds max blocksize " + MAX_SIZE_MEMORY_BLOCK + " of segment: " + this);
		}

		return true;
	}

	/**
	 * Creates a free block
	 *
	 * @param p_address the address
	 * @param p_size    the size
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

				size = size >> 8;
			}

			// Get the corresponding list
			listOffset = m_baseFreeBlockList + getList(p_size) * POINTER_SIZE;

			// Hook block in list
			anchor = readPointer(listOffset);

			// Write pointer to list and successor
			writePointer(p_address + lengthFieldSize, listOffset);
			writePointer(p_address + lengthFieldSize + POINTER_SIZE, anchor);
			if (anchor != 0) {
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
	 * @param p_address the address
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

		if (nextPointer != 0) {
			// Write pointer of successor
			writePointer(nextPointer + lengthFieldSize, prevPointer);
		}
	}

	/**
	 * Gets the suitable list for the given size
	 *
	 * @param p_size the size
	 * @return the suitable list
	 */
	private int getList(final long p_size) {
		int ret = 0;

		while (ret + 1 < m_freeBlockListSizes.length && m_freeBlockListSizes[ret + 1] <= p_size) {
			ret++;
		}

		return ret;
	}

	/**
	 * Read the right part of a marker byte
	 *
	 * @param p_address the address
	 * @return the right part of a marker byte
	 */
	int readRightPartOfMarker(final long p_address) {
		return m_memory.readByte(p_address) & 0xF;
	}

	/**
	 * Read the left part of a marker byte
	 *
	 * @param p_address the address
	 * @return the left part of a marker byte
	 */
	private int readLeftPartOfMarker(final long p_address) {
		return (m_memory.readByte(p_address) & 0xF0) >> 4;
	}

	/**
	 * Extract the size of the length field of the allocated or free area
	 * from the marker byte.
	 *
	 * @param p_marker Marker byte.
	 * @return Size of the length field of block with specified marker byte.
	 */
	private int getSizeFromMarker(final int p_marker) {
		int ret;

		if (p_marker <= OCCUPIED_FLAGS_OFFSET) {
			// free block size
			ret = p_marker;
		} else {
			// allocated block sizes 1, 2, 3 and 5 (chained block next ptr) are used
			ret = p_marker - OCCUPIED_FLAGS_OFFSET;
		}

		return ret;
	}

	/**
	 * Writes a marker byte
	 *
	 * @param p_address the address
	 * @param p_right   the right part
	 */
	private void writeRightPartOfMarker(final long p_address, final int p_right) {
		byte marker;

		marker = (byte) ((m_memory.readByte(p_address) & 0xF0) + (p_right & 0xF));
		m_memory.writeByte(p_address, marker);
	}

	/**
	 * Writes a marker byte
	 *
	 * @param p_address the address
	 * @param p_left    the left part
	 */
	private void writeLeftPartOfMarker(final long p_address, final int p_left) {
		byte marker;

		marker = (byte) (((p_left & 0xF) << 4) + (m_memory.readByte(p_address) & 0xF));
		m_memory.writeByte(p_address, marker);
	}

	/**
	 * Reads a pointer
	 *
	 * @param p_address the address
	 * @return the pointer
	 */
	long readPointer(final long p_address) {
		return read(p_address, POINTER_SIZE);
	}

	/**
	 * Writes a pointer
	 *
	 * @param p_address the address
	 * @param p_pointer the pointer
	 */
	private void writePointer(final long p_address, final long p_pointer) {
		write(p_address, p_pointer, POINTER_SIZE);
	}

	/**
	 * Reads up to 8 bytes combined in a long
	 *
	 * @param p_address the address
	 * @param p_count   the number of bytes
	 * @return the combined bytes
	 */
	protected long read(final long p_address, final int p_count) {
		return m_memory.readVal(p_address, p_count);
	}

	/**
	 * Writes up to 8 bytes combined in a long
	 *
	 * @param p_address the address
	 * @param p_bytes   the combined bytes
	 * @param p_count   the number of bytes
	 */
	private void write(final long p_address, final long p_bytes, final int p_count) {
		m_memory.writeVal(p_address, p_bytes, p_count);
	}

	/**
	 * Read data from the storage into a short array.
	 *
	 * @param p_ptr         Start position in storage.
	 * @param p_array       Array to read the data into.
	 * @param p_arrayOffset Start offset in array to start writing the shorts to.
	 * @param p_length      Number of shorts to read from specified start.
	 * @return Number of read elements.
	 */
	private int readShorts(final long p_ptr, final short[] p_array, final int p_arrayOffset, final int p_length) {
		for (int i = 0; i < p_length; i++) {
			p_array[i + p_arrayOffset] = m_memory.readShort(p_ptr + i * Short.BYTES);
		}

		return p_length;
	}

	/**
	 * Read data from the storage into a int array.
	 *
	 * @param p_ptr         Start position in storage.
	 * @param p_array       Array to read the data into.
	 * @param p_arrayOffset Start offset in array to start writing the ints to.
	 * @param p_length      Number of ints to read from specified start.
	 * @return Number of read elements.
	 */
	private int readInts(final long p_ptr, final int[] p_array, final int p_arrayOffset, final int p_length) {
		for (int i = 0; i < p_length; i++) {
			p_array[i + p_arrayOffset] = m_memory.readInt(p_ptr + i * Integer.BYTES);
		}

		return p_length;
	}

	/**
	 * Read data from the storage into a long array.
	 *
	 * @param p_ptr         Start position in storage.
	 * @param p_array       Array to read the data into.
	 * @param p_arrayOffset Start offset in array to start writing the longs to.
	 * @param p_length      Number of longs to read from specified start.
	 * @return Number of read elements.
	 */
	private int readLongs(final long p_ptr, final long[] p_array, final int p_arrayOffset, final int p_length) {
		for (int i = 0; i < p_length; i++) {
			p_array[i + p_arrayOffset] = m_memory.readLong(p_ptr + i * Long.BYTES);
		}

		return p_length;
	}

	/**
	 * Write an array of shorts to the storage.
	 *
	 * @param p_ptr         Start address to write to.
	 * @param p_array       Array with data to write.
	 * @param p_arrayOffset Offset in array to start reading the data from.
	 * @param p_length      Number of elements to write.
	 * @return Number of written elements
	 */
	private int writeShorts(final long p_ptr, final short[] p_array, final int p_arrayOffset, final int p_length) {
		for (int i = 0; i < p_length; i++) {
			m_memory.writeShort(p_ptr + i * Short.BYTES, p_array[i + p_arrayOffset]);
		}

		return p_length;
	}

	/**
	 * Write an array of ints to the storage.
	 *
	 * @param p_ptr         Start address to write to.
	 * @param p_array       Array with data to write.
	 * @param p_arrayOffset Offset in array to start reading the data from.
	 * @param p_length      Number of elements to write.
	 * @return Number of written elements
	 */
	private int writeInts(final long p_ptr, final int[] p_array, final int p_arrayOffset, final int p_length) {
		for (int i = 0; i < p_length; i++) {
			m_memory.writeInt(p_ptr + i * Integer.BYTES, p_array[i + p_arrayOffset]);
		}

		return p_length;
	}

	/**
	 * Write an array of longs to the storage.
	 *
	 * @param p_ptr         Start address to write to.
	 * @param p_array       Array with data to write.
	 * @param p_arrayOffset Offset in array to start reading the data from.
	 * @param p_length      Number of elements to write.
	 * @return Number of written elements
	 */
	private int writeLongs(final long p_ptr, final long[] p_array, final int p_arrayOffset, final int p_length) {
		for (int i = 0; i < p_length; i++) {
			m_memory.writeLong(p_ptr + i * Long.BYTES, p_array[i + p_arrayOffset]);
		}

		return p_length;
	}

	// --------------------------------------------------------------------------------------

	// Classes

	/**
	 * Holds fragmentation information of a segment
	 *
	 * @author Florian Klein 10.04.2014
	 */
	public final class Status {

		// Attributes
		private long m_freeSpace;
		private int m_freeBlocks;
		private int m_freeSmall64ByteBlocks;

		// Constructors

		/**
		 * Creates an instance of Status
		 *
		 * @param p_freeSpace the free space
		 */
		private Status(final long p_freeSpace) {
			m_freeSpace = p_freeSpace;
			m_freeBlocks = 1;
			m_freeSmall64ByteBlocks = 0;
		}

		// Getters

		/**
		 * Gets the free space
		 *
		 * @return the freeSpace
		 */
		long getFreeSpace() {
			return m_freeSpace;
		}

		/**
		 * Gets the number of free blocks
		 *
		 * @return the freeBlocks
		 */
		int getFreeBlocks() {
			return m_freeBlocks;
		}

		/**
		 * Gets the number of small blocks
		 *
		 * @return the smallBlocks
		 */
		int getSmallBlocks() {
			return m_freeSmall64ByteBlocks;
		}

		@Override
		public String toString() {
			return "Status [m_freeSpace=" + m_freeSpace + ", m_freeBlocks=" + m_freeBlocks + ", m_smallBlocks="
					+ m_freeSmall64ByteBlocks + "]";
		}

	}

}
