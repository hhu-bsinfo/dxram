package de.hhu.bsinfo.soh;

import java.util.Arrays;

/**
 * The memory is divided into segments to enable better concurrent access
 * to the memory as a a whole. This also enables locking single segmens
 * for house keeping tasks such as defragmentation.
 *
 * @author Florian Klein 04.04.2014
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.11.2015
 */
public final class SmallObjectHeapSegment {

	// Constants
	public static final byte POINTER_SIZE = 5;
	public static final byte SMALL_BLOCK_SIZE = 64;
	public static final byte OCCUPIED_FLAGS_OFFSET = 0x5;
	public static final byte OCCUPIED_FLAGS_OFFSET_MASK = 0x03;
	public static final byte SINGLE_BYTE_MARKER = 0xF;

	// limits a single block of memory to 16777216 bytes i.e 16MB
	public static final int MAX_LENGTH_FIELD = 3;
	public static final int MAX_SIZE_MEMORY_BLOCK = (int) Math.pow(2, 8 * MAX_LENGTH_FIELD);
	public static final int SIZE_MARKER_BYTE = 1;

	// Attributes, have them accessible by the package to enable walking and analyzing the segment
	// don't modify or access them otherwise
	Storage m_memory;
	int m_segmentID;
	long m_base;
	long m_baseFreeBlockList;
	Status m_status;
	long m_assignedThread;

	//JNIReadWriteSpinLock m_lock;

	long[] m_freeBlockListSizes;

	long m_size = -1;
	long m_fullSize = -1;
	int m_freeBlocksListCount = -1;
	int m_freeBlocksListSizePerSegment = -1;

	// Constructors
	/**
	 * Creates an instance of Segment
	 * @param p_memory The underlying storage to use for this segment.
	 * @param p_segmentID The ID of the segment
	 * @param p_base The base address of the segment
	 * @param p_size The size of the segment in bytes.
	 * @throws MemoryException If creating segement failed.
	 */
	public SmallObjectHeapSegment(final Storage p_memory, final int p_segmentID, final long p_base, final long p_size) {
		m_memory = p_memory;
		m_segmentID = p_segmentID;
		m_assignedThread = 0;
		m_base = p_base;
		m_fullSize = p_size;

		//m_lock = new JNIReadWriteSpinLock();

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
	 * @return Size in bytes.
	 */
	public long getSize() {
		return m_fullSize;
	}

	/**
	 * Gets the status
	 * @return the status
	 */
	public Status getStatus() {
		return m_status.copy();
	}

	/**
	 * Checks if the Segment is assigned to a thread.
	 * @return true if the Segment is assigned, false otherwise
	 */
	public boolean isAssigned() {
		return m_assignedThread != 0;
	}

	/**
	 * Assigns the Segment to a thread.
	 * @param p_threadID
	 *            The thread ID to assign this segment to.
	 * @return the previously assigned thread
	 */
	public long assign(final long p_threadID) {
		long ret;

		ret = m_assignedThread;
		m_assignedThread = p_threadID;

		return ret;
	}

	/**
	 * Unassigns the Segment
	 */
	public void unassign() {
		m_assignedThread = 0;
	}

	/**
	 * Gets the current fragmentation in percentage
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
	 * @param p_size
	 *            the size of the block in bytes.
	 * @return the address of the block
	 * @throws MemoryException If allocating memory in segment failed.
	 */
	public long malloc(final int p_size) {
		long ret = -1;
		int list;
		long address;
		long blockSize;
		int lengthFieldSize;
		long freeSize;
		int freeLengthFieldSize;
		byte blockMarker;

		if (p_size > 0 && p_size <= MAX_SIZE_MEMORY_BLOCK) {
			if (p_size >= 1 << 16) {
				lengthFieldSize = 3;
			} else if (p_size >= 1 << 8) {
				lengthFieldSize = 2;
			} else {
				lengthFieldSize = 1;
			}
			blockSize = p_size + lengthFieldSize;
			blockMarker = (byte) (OCCUPIED_FLAGS_OFFSET + lengthFieldSize);

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

				// Write block size
				write(address, p_size, lengthFieldSize);

				ret = address;
			}
		}

		return ret;
	}

	/**
	 * Allocate a large memory block for multiple objects
	 * @param p_sizes
	 *            the sizes of the objects in bytes
	 * @return the offsets of the objects
	 * @throws MemoryException
	 *             if the memory block could not be allocated
	 */
	public long[] malloc(final int... p_sizes) {
		long[] ret;
		long address;
		int size;
		int lengthfieldSize;
		byte marker;
		boolean oneByteOverhead = false;

		assert p_sizes != null;
		assert p_sizes.length > 0;

		ret = new long[p_sizes.length];
		Arrays.fill(ret, 0);

		size = getRequiredMemory(p_sizes);
		if (size >= 1 << 16) {
			lengthfieldSize = 3;
			if (size - 3 < 1 << 16) {
				oneByteOverhead = true;
			}
		} else if (size >= 1 << 8) {
			lengthfieldSize = 2;
			if (size - 2 < 1 << 8) {
				oneByteOverhead = true;
			}
		} else {
			lengthfieldSize = 1;
		}
		size -= lengthfieldSize;

		address = malloc(size) - 1;

		if (oneByteOverhead) {
			writeRightPartOfMarker(address, SINGLE_BYTE_MARKER);
			address++;
			writeLeftPartOfMarker(address, SINGLE_BYTE_MARKER);
		}

		for (int i = 0; i < p_sizes.length; i++) {
			if (p_sizes[i] > 0) {
				if (p_sizes[i] >= 1 << 16) {
					lengthfieldSize = 3;
				} else if (p_sizes[i] >= 1 << 8) {
					lengthfieldSize = 2;
				} else {
					lengthfieldSize = 1;
				}
				marker = (byte) (OCCUPIED_FLAGS_OFFSET + lengthfieldSize);

				writeRightPartOfMarker(address, marker);
				address += 1;

				ret[i] = address;

				write(address, p_sizes[i], lengthfieldSize);
				address += lengthfieldSize;
				address += p_sizes[i];

				writeLeftPartOfMarker(address, marker);
			}
		}

		return ret;
	}

	/**
	 * Frees a memory block
	 * @param p_address
	 *            the address of the block
	 * @throws MemoryException If free'ing memory in segment failed.
	 */
	public void free(final long p_address) {
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

		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
		blockSize = getSizeMemoryBlock(p_address);

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
		} else if (!leftFree && rightFree) {
			m_status.m_freeSpace += blockSize + lengthFieldSize + SIZE_MARKER_BYTE;
			if (blockSize + lengthFieldSize + rightSize >= SMALL_BLOCK_SIZE && rightSize < SMALL_BLOCK_SIZE) {
				m_status.m_freeSmall64ByteBlocks--;
			}
		} else if (leftFree && rightFree) {
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
	}
	
	/**
	 * Get the size of an allocated block of memory.
	 * @param p_address Address of the block.
	 * @return Size of the block in bytes (payload only).
	 * @throws MemoryException If getting size failed.
	 */
	public int getSizeBlock(final long p_address) {
		int lengthFieldSize;
		int size;
		
		assert assertSegmentBounds(p_address);

		// skip length byte(s)
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
		size = (int) read(p_address, lengthFieldSize);
		
		return size;
	}

	/**
	 * Overwrites the bytes in the memory with the given value
	 * @param p_address
	 *            the address to start
	 * @param p_size
	 *            the number of bytes to overwrite
	 * @param p_value
	 *            the value to write
	 * @throws MemoryException
	 *             if the memory could not be set
	 */
	public void set(final long p_address, final long p_size, final byte p_value) {
		assert assertSegmentBounds(p_address, p_size);
		assert assertSegmentMaxBlocksize(p_size);

		int lengthFieldSize;

		// skip length byte(s)
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

		m_memory.set(p_address + lengthFieldSize, p_size, p_value);
	}

	/**
	 * Read a single byte from the specified address.
	 * @param p_address
	 *            Address.
	 * @return Byte read.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public byte readByte(final long p_address) {
		return readByte(p_address, 0);
	}

	/**
	 * Read a single byte from the specified address + offset.
	 * @param p_address
	 *            Address.
	 * @param p_offset
	 *            Offset to add to the address.
	 * @return Byte read.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public byte readByte(final long p_address, final long p_offset) {
		assert assertSegmentBounds(p_address, p_offset);

		int lengthFieldSize;
		int size;

		// skip length byte(s)
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
		size = (int) read(p_address, lengthFieldSize);

		assert p_offset < size;
		assert size >= Byte.BYTES && p_offset + Byte.BYTES <= size;
		if (!(p_offset < size) || !(size >= Byte.BYTES && p_offset + Byte.BYTES <= size))
			return 0;

		return m_memory.readByte(p_address + lengthFieldSize + p_offset);
	}

	/**
	 * Read a single short from the specified address.
	 * @param p_address
	 *            Address
	 * @return Short read.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public short readShort(final long p_address) {
		return readShort(p_address, 0);
	}

	/**
	 * Read a single short from the specified address + offset.
	 * @param p_address
	 *            Address.
	 * @param p_offset
	 *            Offset to add to the address.
	 * @return Short read.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public short readShort(final long p_address, final long p_offset) {
		assert assertSegmentBounds(p_address, p_offset);

		int lengthFieldSize;
		int size;

		// skip length byte(s)
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
		size = (int) read(p_address, lengthFieldSize);

		assert p_offset < size;
		assert size >= Short.BYTES && p_offset + Short.BYTES <= size;
		if (!(p_offset < size) || !(size >= Short.BYTES && p_offset + Short.BYTES <= size))
			return 0;

		return m_memory.readShort(p_address + lengthFieldSize + p_offset);
	}

	/**
	 * Read a single int from the specified address.
	 * @param p_address
	 *            Address.
	 * @return Int read.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public int readInt(final long p_address) {
		return readInt(p_address, 0);
	}

	/**
	 * Read a single int from the specified address + offset.
	 * @param p_address
	 *            Address.
	 * @param p_offset
	 *            Offset to add to the address.
	 * @return Int read.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public int readInt(final long p_address, final long p_offset) {
		assert assertSegmentBounds(p_address, p_offset);

		int lengthFieldSize;
		int size;

		// skip length byte(s)
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
		size = (int) read(p_address, lengthFieldSize);

		assert p_offset < size;
		assert size >= Integer.BYTES && p_offset + Integer.BYTES <= size;
		if (!(p_offset < size) || !(size >= Integer.BYTES && p_offset + Integer.BYTES <= size))
			return 0;

		return m_memory.readInt(p_address + lengthFieldSize + p_offset);
	}

	/**
	 * Read a long from the specified address.
	 * @param p_address
	 *            Address.
	 * @return Long read.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public long readLong(final long p_address) {
		return readLong(p_address, 0);
	}

	/**
	 * Read a long from the specified address + offset.
	 * @param p_address
	 *            Address.
	 * @param p_offset
	 *            Offset to add to the address.
	 * @return Long read.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public long readLong(final long p_address, final long p_offset) {
		assert assertSegmentBounds(p_address, p_offset);

		int lengthFieldSize;
		int size;

		// skip length byte(s)
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
		size = (int) read(p_address, lengthFieldSize);

		assert p_offset < size;
		assert size >= Long.BYTES || p_offset + Long.BYTES <= size;
		if (!(p_offset < size) || !(size >= Long.BYTES || p_offset + Long.BYTES <= size))
			return 0;
		
		return m_memory.readLong(p_address + lengthFieldSize + p_offset);
	}

	/**
	 * Reads a block of bytes from the memory
	 * @param p_address
	 *            the address to start reading
	 * @return the read bytes
	 * @throws MemoryException
	 *             if the bytes could not be read
	 */
	public byte[] readBytes(final long p_address) {
		return readBytes(p_address, 0);
	}

	/**
	 * Read a block from memory. This will read bytes until the end
	 * of the allocated block, starting address + offset.
	 * @param p_address
	 *            Address of allocated block.
	 * @param p_offset
	 *            Offset added to address for start address to read from.
	 * @return Byte array with read data.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public byte[] readBytes(final long p_address, final long p_offset) {
		assert assertSegmentBounds(p_address, p_offset);

		byte[] ret;
		int lengthFieldSize;
		int size;
		long readStartOffset;

		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
		size = (int) read(p_address, lengthFieldSize);

		assert p_offset < size;
		readStartOffset = p_address + lengthFieldSize + p_offset;

		ret = new byte[(int) (size - p_offset)];
		m_memory.readBytes(readStartOffset, ret, 0, ret.length);

		return ret;
	}
	
	public int readBytes(final long p_address, final long p_offset, final byte[] p_buffer, int p_offsetArray, int p_length) 
	{
		assert assertSegmentBounds(p_address, p_offset);

		int bytesRead = -1;
		int lengthFieldSize;
		int size;
		long readStartOffset;

		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
		size = (int) read(p_address, lengthFieldSize);

		assert p_offset < size;
		readStartOffset = p_address + lengthFieldSize + p_offset;

		bytesRead = m_memory.readBytes(readStartOffset, p_buffer, p_offsetArray, p_length);

		return bytesRead;
	}

	/**
	 * Write a single byte to the specified address.
	 * @param p_address
	 *            Address.
	 * @param p_value
	 *            Byte to write.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public void writeByte(final long p_address, final byte p_value) {
		writeByte(p_address, 0, p_value);
	}

	/**
	 * Write a single byte to the specified address + offset.
	 * @param p_address
	 *            Address.
	 * @param p_offset
	 *            Offset to add to the address.
	 * @param p_value
	 *            Byte to write.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public void writeByte(final long p_address, final long p_offset, final byte p_value) {
		assert assertSegmentBounds(p_address, p_offset);

		int lengthFieldSize;
		int size;

		// skip length byte(s)
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
		size = (int) read(p_address, lengthFieldSize);

		assert p_offset < size;
		assert size >= Byte.BYTES && p_offset + Byte.BYTES <= size;

		m_memory.writeByte(p_address + lengthFieldSize + p_offset, p_value);
	}

	/**
	 * Write a single short to the specified address.
	 * @param p_address
	 *            Address.
	 * @param p_value
	 *            Short to write.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public void writeShort(final long p_address, final short p_value) {
		writeShort(p_address, 0, p_value);
	}

	/**
	 * Write a short to the spcified address + offset.
	 * @param p_address
	 *            Address.
	 * @param p_offset
	 *            Offset to add to the address.
	 * @param p_value
	 *            Short to write.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public void writeShort(final long p_address, final long p_offset, final short p_value) {
		assert assertSegmentBounds(p_address, p_offset);

		int lengthFieldSize;
		int size;

		// skip length byte(s)
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
		size = (int) read(p_address, lengthFieldSize);

		assert p_offset < size;
		assert size >= Short.BYTES && p_offset + Short.BYTES <= size;

		m_memory.writeShort(p_address + lengthFieldSize + p_offset, p_value);
	}

	/**
	 * Write a single int to the specified address.
	 * @param p_address
	 *            Address.
	 * @param p_value
	 *            Int to write.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public void writeInt(final long p_address, final int p_value) {
		writeInt(p_address, 0, p_value);
	}

	/**
	 * Write a single int to the specified address + offset.
	 * @param p_address
	 *            Address.
	 * @param p_offset
	 *            Offset to add to the address.
	 * @param p_value
	 *            int to write.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public void writeInt(final long p_address, final long p_offset, final int p_value) {
		assert assertSegmentBounds(p_address, p_offset);

		int lengthFieldSize;
		int size;

		// skip length byte(s)
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
		size = (int) read(p_address, lengthFieldSize);

		assert p_offset < size;
		assert size >= Integer.BYTES && p_offset + Integer.BYTES <= size;

		m_memory.writeInt(p_address + lengthFieldSize + p_offset, p_value);
	}

	/**
	 * Write a long value to the specified address.
	 * @param p_address
	 *            Address.
	 * @param p_value
	 *            Long value to write.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public void writeLong(final long p_address, final long p_value) {
		writeLong(p_address, 0, p_value);
	}

	/**
	 * Write a long value to the specified address + offset.
	 * @param p_address
	 *            Address.
	 * @param p_offset
	 *            Offset to add to the address.
	 * @param p_value
	 *            Long value to write.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public void writeLong(final long p_address, final long p_offset, final long p_value) {
		assert assertSegmentBounds(p_address, p_offset);

		int lengthFieldSize;
		int size;

		// skip length byte(s)
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
		size = (int) read(p_address, lengthFieldSize);

		assert p_offset < size;
		assert size >= Long.BYTES && p_offset + Long.BYTES <= size;

		m_memory.writeLong(p_address + lengthFieldSize + p_offset, p_value);
	}

	/**
	 * Write an array of bytes to the specified address.
	 * @param p_address
	 *            Address.
	 * @param p_value
	 *            Bytes to write.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public int writeBytes(final long p_address, final byte[] p_value) {
		return writeBytes(p_address, 0, p_value);
	}

	/**
	 * Write an array of bytes to the specified address + offset.
	 * @param p_address
	 *            Address.
	 * @param p_offset
	 *            Offset to add to the address.
	 * @param p_value
	 *            Bytes to write.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public int writeBytes(final long p_address, final long p_offset, final byte[] p_value) {
		return writeBytes(p_address, p_offset, p_value, 0, p_value.length);
	}

	/**
	 * Write an array of bytes to the specified address + offset.
	 * @param p_address
	 *            Address.
	 * @param p_offset
	 *            Offset to add to the address.
	 * @param p_value
	 *            Bytes to write.
	 * @param p_length
	 * 				Number of bytes to write.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public int writeBytes(final long p_address, final long p_offset, final byte[] p_value, final int p_offsetArray, final int p_length) {
		assert assertSegmentBounds(p_address, p_offset);

		int bytesWritten = -1;
		int lengthFieldSize;
		int size;
		long offset;

		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
		size = (int) read(p_address, lengthFieldSize);

		assert p_offset < size;
		assert p_offset + p_length <= size;

		offset = p_address + lengthFieldSize + p_offset;
			
		bytesWritten = m_memory.writeBytes(offset, p_value, p_offsetArray, p_length);
		
		return bytesWritten;
	}

	/**
	 * Get the user definable state of a specified address referring
	 * a malloc'd block of memory.
	 * @param p_address
	 *            Address of malloc'd block of memory.
	 * @return User definable state stored for that block (valid values: 0, 1, 2. invalid: -1)
	 * @throws MemoryException If reading memory fails.
	 */
	public int getCustomState(final long p_address) {
		int marker;
		int ret;

		assert assertSegmentBounds(p_address);

		marker = readRightPartOfMarker(p_address - SIZE_MARKER_BYTE);
		if (marker == SINGLE_BYTE_MARKER || marker <= OCCUPIED_FLAGS_OFFSET) {
			// invalid
			ret = -1;
		} else {
			ret = (byte) ((marker - OCCUPIED_FLAGS_OFFSET - 1) / OCCUPIED_FLAGS_OFFSET_MASK);
		}

		return ret;
	}

	/**
	 * Set the user definable state for a specified address referring
	 * a malloc'd block of memory.
	 * @param p_address
	 *            Address of malloc'd block of memory.
	 * @param p_customState
	 *            State to set for that block of memory (valid values: 0, 1, 2.
	 *            all other values invalid).
	 * @throws MemoryException If reading or writing memory fails.
	 */
	public void setCustomState(final long p_address, final int p_customState) {
		int marker;
		int lengthFieldSize;
		int size;

		assert assertSegmentBounds(p_address);

		if (!(p_customState >= 0 && p_customState < 3)) {
			throw new MemoryRuntimeException("Custom state (" + p_customState + ") out of range, addr: " + p_address);
		}

		marker = readRightPartOfMarker(p_address - SIZE_MARKER_BYTE);

		if (!(marker != SINGLE_BYTE_MARKER && marker > OCCUPIED_FLAGS_OFFSET)) {
			throw new MemoryRuntimeException("Invalid marker " + marker + " at address " + p_address);
		}

		lengthFieldSize = getSizeFromMarker(marker);
		size = (int) read(p_address, lengthFieldSize);
		marker = (OCCUPIED_FLAGS_OFFSET + lengthFieldSize) + (p_customState * 3);

		writeRightPartOfMarker(p_address - SIZE_MARKER_BYTE, marker);
		writeLeftPartOfMarker(p_address + lengthFieldSize + size, marker);
	}

	/**
	 * Get the size of the allocated or free'd block of memory specified
	 * by the given address.
	 * @param p_address
	 *            Address of block to get the size of.
	 * @return Size of memory block at specified address.
	 * @throws MemoryException If reading memory fails.
	 */
	public long getSizeMemoryBlock(final long p_address) {
		int lengthFieldSize;

		assert assertSegmentBounds(p_address);

		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
		return read(p_address, lengthFieldSize);
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

	/**
	 * Calculates the required memory for multiple objects
	 * @param p_sizes
	 *            the sizes od the objects
	 * @return the size of the required memory
	 */
	public static int getRequiredMemory(final int... p_sizes) {
		int ret;

		assert p_sizes != null;
		assert p_sizes.length > 0;

		ret = 0;
		for (final int size : p_sizes) {
			if (size > 0) {
				ret += size;

				if (size >= 1 << 16) {
					ret += 3;
				} else if (size >= 1 << 8) {
					ret += 2;
				} else {
					ret += 1;
				}

				ret += 1;
			}
		}
		ret -= 1;

		return ret;
	}

	// -------------------------------------------------------------------------------------------

	/**
	 * Check the segment bounds with the specified address.
	 * @param p_address Address to check if within segment.
	 * @throws MemoryException If address not within segment bounds.
	 */
	private boolean assertSegmentBounds(final long p_address) {
		if (p_address < m_base || p_address > m_base + m_fullSize) {
			throw new MemoryRuntimeException("Address " + p_address + " is not within segment: " + this);
		}
		
		return true;
	}

	/**
	 * Check the segment bounds with the specified start address and size.
	 * @param p_address Address to check if within bounds.
	 * @param p_length Number of bytes starting at address.
	 * @throws MemoryException If address and specified length not within segment bounds.
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
	 * @param p_size Size to check if not exceeding max blocksize.
	 * @throws MemoryException If size exceeds max blocksize.
	 */
	public boolean assertSegmentMaxBlocksize(final long p_size) {
		if (p_size > MAX_SIZE_MEMORY_BLOCK) {
			throw new MemoryRuntimeException("Size " + p_size
					+ " exceeds max blocksize " + MAX_SIZE_MEMORY_BLOCK + " of segment: " + this);
		}
		
		return true;
	}

	/**
	 * Creates a free block
	 * @param p_address
	 *            the address
	 * @param p_size
	 *            the size
	 * @throws MemoryException If creating free block failed.
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
	 * @param p_address
	 *            the address
	 * @throws MemoryException If unhooking free block failed.
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
	 * @param p_size
	 *            the size
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
	 * @param p_address
	 *            the address
	 * @return the right part of a marker byte
	 * @throws MemoryException If reading fails.
	 */
	protected int readRightPartOfMarker(final long p_address) {
		return m_memory.readByte(p_address) & 0xF;
	}

	/**
	 * Read the left part of a marker byte
	 * @param p_address
	 *            the address
	 * @return the left part of a marker byte
	 * @throws MemoryException If reading fails.
	 */
	protected int readLeftPartOfMarker(final long p_address) {
		return (m_memory.readByte(p_address) & 0xF0) >> 4;
	}

	/**
	 * Extract the size of the length field of the allocated or free area
	 * from the marker byte.
	 *
	 * @param p_marker Marker byte.
	 * @return Size of the length field of block with specified marker byte.
	 */
	protected int getSizeFromMarker(final int p_marker) {
		int ret;

		if (p_marker <= OCCUPIED_FLAGS_OFFSET) {
			// free block size
			ret = p_marker;
		} else {
			ret = ((p_marker - OCCUPIED_FLAGS_OFFSET - 1) % OCCUPIED_FLAGS_OFFSET_MASK) + 1;
		}

		return ret;
	}

	/**
	 * Writes a marker byte
	 * @param p_address
	 *            the address
	 * @param p_right
	 *            the right part
	 * @throws MemoryException If reading fails.
	 */
	private void writeRightPartOfMarker(final long p_address, final int p_right) {
		byte marker;

		marker = (byte) ((m_memory.readByte(p_address) & 0xF0) + (p_right & 0xF));
		m_memory.writeByte(p_address, marker);
	}

	/**
	 * Writes a marker byte
	 * @param p_address
	 *            the address
	 * @param p_left
	 *            the left part
	 * @throws MemoryException If reading fails.
	 */
	private void writeLeftPartOfMarker(final long p_address, final int p_left) {
		byte marker;

		marker = (byte) (((p_left & 0xF) << 4) + (m_memory.readByte(p_address) & 0xF));
		m_memory.writeByte(p_address, marker);
	}

	/**
	 * Reads a pointer
	 * @param p_address
	 *            the address
	 * @return the pointer
	 * @throws MemoryException If reading pointer failed.
	 */
	protected long readPointer(final long p_address) {
		return read(p_address, POINTER_SIZE);
	}

	/**
	 * Writes a pointer
	 * @param p_address
	 *            the address
	 * @param p_pointer
	 *            the pointer
	 * @throws MemoryException If writing pointer failed.
	 */
	private void writePointer(final long p_address, final long p_pointer) {
		write(p_address, p_pointer, POINTER_SIZE);
	}

	/**
	 * Reads up to 8 bytes combined in a long
	 * @param p_address
	 *            the address
	 * @param p_count
	 *            the number of bytes
	 * @return the combined bytes
	 * @throws MemoryException If reading failed.
	 */
	protected long read(final long p_address, final int p_count) {
		return m_memory.readVal(p_address, p_count);
	}

	/**
	 * Writes up to 8 bytes combined in a long
	 * @param p_address
	 *            the address
	 * @param p_bytes
	 *            the combined bytes
	 * @param p_count
	 *            the number of bytes
	 * @throws MemoryException If writing failed.
	 */
	private void write(final long p_address, final long p_bytes, final int p_count) {
		m_memory.writeVal(p_address, p_bytes, p_count);
	}

	// --------------------------------------------------------------------------------------

	// Classes
	/**
	 * Holds fragmentation information of a segment
	 * @author Florian Klein 10.04.2014
	 */
	public final class Status {

		// Attributes
		private long m_freeSpace;
		private int m_freeBlocks;
		private int m_freeSmall64ByteBlocks;
		private long[] m_sizes;
		private long[] m_blocks;

		// Constructors
		/**
		 * Creates an instance of Status
		 * @param p_freeSpace
		 *            the free space
		 */
		private Status(final long p_freeSpace) {
			m_freeSpace = p_freeSpace;
			m_freeBlocks = 1;
			m_freeSmall64ByteBlocks = 0;
		}

		// Getters
		/**
		 * Gets the free space
		 * @return the freeSpace
		 */
		public long getFreeSpace() {
			return m_freeSpace;
		}

		/**
		 * Gets the number of free blocks
		 * @return the freeBlocks
		 */
		public int getFreeBlocks() {
			return m_freeBlocks;
		}

		/**
		 * Gets the number of small blocks
		 * @return the smallBlocks
		 */
		public int getSmallBlocks() {
			return m_freeSmall64ByteBlocks;
		}

		/**
		 * Gets the sizes
		 * @return the sizes
		 */
		public long[] getSizes() {
			return m_sizes;
		}

		/**
		 * Gets the blocks
		 * @return the blocks
		 */
		public long[] getBlocks() {
			return m_blocks;
		}

		// Methods
		/**
		 * Creates a copy
		 * @return the copy
		 */
		private Status copy() {
			Status ret;

			ret = new Status(0);
			ret.m_freeSpace = m_freeSpace;
			ret.m_freeBlocks = m_freeBlocks;
			ret.m_freeSmall64ByteBlocks = m_freeSmall64ByteBlocks;

			ret.m_sizes = m_freeBlockListSizes;
			// ret.m_blocks = new long[0];
			// ret.m_blocks = getBlocks(p_pointerOffset);

			return ret;
		}

		// /**
		// * Gets all free blocks
		// * @param p_pointerOffset
		// * the pointer offset
		// * @return all free blocks
		// */
		// private long[] getBlocks(final long p_pointerOffset) {
		// long[] ret;
		// long address;
		// int lengthFieldSize;
		// long count;
		//
		// ret = new long[m_listSizes.length];
		// for (int i = 0;i < m_listSizes.length;i++) {
		// count = 0;
		//
		// address = readPointer(p_pointerOffset + i * POINTER_SIZE);
		// while (address != 0) {
		// count++;
		//
		// lengthFieldSize = readRightPartOfMarker(address - 1);
		// address = readPointer(address + lengthFieldSize + POINTER_SIZE);
		// }
		//
		// ret[i] = count;
		// }
		//
		// return ret;
		// }

		@Override
		public String toString() {
			return "Status [m_freeSpace=" + m_freeSpace + ", m_freeBlocks=" + m_freeBlocks + ", m_smallBlocks=" + m_freeSmall64ByteBlocks + "]";
		}

	}

}