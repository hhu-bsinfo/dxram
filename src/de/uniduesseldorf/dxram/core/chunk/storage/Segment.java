package de.uniduesseldorf.dxram.core.chunk.storage;

import java.util.Arrays;
import java.util.concurrent.locks.ReadWriteLock;

import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.utils.Contract;
import de.uniduesseldorf.dxram.utils.locks.JNIReadWriteSpinLock;

/**
 * Represents a segment of the memory
 * @author Florian Klein 04.04.2014
 */
public final class Segment {

	// Constants
	public static final byte POINTER_SIZE = 5;
	public static final byte SMALL_BLOCK_SIZE = 64;
	public static final byte OCCUPIED_FLAGS_OFFSET = 0x5;
	public static final byte OCCUPIED_FLAGS_OFFSET_MASK = 0x03;
	public static final byte SINGLE_BYTE_MARKER = 0xF;

	// limits a single block of memory to 16777216 bytes i.e 16MB
	public static final int MAX_LENGTH_FIELD = 3;
	public static final int MAX_SIZE_MEMORY_BLOCK = (int) Math.pow(2, 8 * MAX_LENGTH_FIELD);
	
	// Attributes
	private Storage m_memory;
	private int m_segmentID;
	private long m_base;
	private long m_baseFreeBlockList;
	private Status m_status;
	private long m_assignedThread;

	private ReadWriteLock m_lock;
	
	private long[] m_freeBlockListSizes;

	private long m_size = -1;
	private long m_fullSize = -1;
	private int m_freeBlocksListCount = -1;
	private int m_freeBlocksListSizePerSegment = -1;
	
	// Constructors
	/**
	 * Creates an instance of Segment
	 * @param p_segmentID
	 *            the ID of the segment
	 * @param p_base
	 *            the base address of the segment
	 * @param p_size
	 *            the size of the segment
	 * @throws MemoryException If creating segement block failed.
	 */
	public Segment(final Storage p_memory, final int p_segmentID, final long p_base, final long p_size) throws MemoryException {
		m_memory = p_memory;
		m_segmentID = p_segmentID;
		m_assignedThread = 0;
		m_base = p_base;
		m_fullSize = p_size;

		m_lock = new JNIReadWriteSpinLock();

		// according to segment size, have a proper amount of
		// free memory block lists
		// -1, because we don't need a free block list for the full segment
		// detect highest bit using log2 to have proper segment sizes
		m_freeBlocksListCount = (int) (Math.log(p_size) / Math.log(2)) - 1; 
		m_freeBlocksListSizePerSegment = m_freeBlocksListCount * POINTER_SIZE;// + 3;
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
		createFreeBlock(p_base + 1, m_size - 2); 
		m_status = new Status(m_size - 2);
	}

	/**
	 * Gets the status
	 * @return the status
	 */
	public Status getStatus() {
		return m_status.copy();
	}

	/**
	 * Checks if the Segment is assigned
	 * @return true if the Segment is assigned, false otherwise
	 */
	public boolean isAssigned() {
		return m_assignedThread != 0;
	}

	/**
	 * Assigns the Segment
	 * @param p_threadID
	 *            the assigned thread
	 * @return the previous assigned thread
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


	public void lockAccess() {
		m_lock.readLock().lock();
	}


	public boolean tryLockAccess() {
		return m_lock.readLock().tryLock();
	}


	public void unlockAccess() {
		m_lock.readLock().unlock();
	}

	public void lockManage() {
		m_lock.writeLock().lock();
	}
	
	public boolean tryLockManage() {
		return m_lock.writeLock().tryLock();
	}
	
	public void unlockManage() {
		m_lock.writeLock().unlock();
	}

	/**
	 * Gets the current fragmentation
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
	 *            the size of the block
	 * @return the offset of the block
	 * @throws MemoryException If allocating memory in segment failed.
	 */
	public long malloc(final int p_size) throws MemoryException {
		long ret = -1;
		int list;
		long address;
		long blockSize;
		int lengthFieldSize;
		long freeSize;
		int freeLengthFieldSize;
		byte blockMarker;
		
		if (p_size <= 0 || p_size > MAX_SIZE_MEMORY_BLOCK)
			return -1;
		
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

			freeLengthFieldSize = readRightPartOfMarker(address - 1);
			freeSize = read(address, freeLengthFieldSize);
			if (freeSize == blockSize) {
				m_status.m_freeSpace -= blockSize;
				m_status.m_freeBlocks--;
				if (freeSize < SMALL_BLOCK_SIZE) {
					m_status.m_smallBlocks--;
				}
			} else if (freeSize == blockSize + 1) {
				// 1 Byte to big -> write two markers on the right
				writeRightPartOfMarker(address + blockSize, SINGLE_BYTE_MARKER);
				writeLeftPartOfMarker(address + blockSize + 1, SINGLE_BYTE_MARKER);

				// +1 for the marker byte added
				m_status.m_freeSpace -= blockSize + 1; 
				m_status.m_freeBlocks--;
				if (freeSize + 1 < SMALL_BLOCK_SIZE) {
					m_status.m_smallBlocks--;
				}
			} else {
				// Block is to big -> create a new free block with the remaining size
				createFreeBlock(address + blockSize + 1, freeSize - blockSize - 1);

				// +1 for the marker byte added
				m_status.m_freeSpace -= blockSize + 1;
				if (freeSize >= SMALL_BLOCK_SIZE && freeSize - blockSize - 1 < SMALL_BLOCK_SIZE) {
					m_status.m_smallBlocks++;
				}
			}
			// Write marker
			writeLeftPartOfMarker(address + blockSize, blockMarker);
			writeRightPartOfMarker(address - 1, blockMarker);

			// Write block size
			write(address, p_size, lengthFieldSize);

			MemoryStatistic.getInstance().malloc(blockSize);

			ret = address;
		}

		return ret;
	}
	
	/**
	 * Allocate a large memory block for multiple objects
	 * @param p_sizes
	 *            the sizes of the objects
	 * @return the offsets of the objects
	 * @throws MemoryException
	 *             if the memory block could not be allocated
	 */
	public long[] malloc(final int... p_sizes) throws MemoryException {
		long[] ret;
		long address;
		int size;
		int lengthfieldSize;
		byte marker;
		boolean oneByteOverhead = false;

		Contract.checkNotNull(p_sizes, "no sizes given");
		Contract.check(p_sizes.length > 0, "no sizes given");

		ret = new long[p_sizes.length];
		Arrays.fill(ret, 0);

		try {
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
		} catch (final Throwable e) {
			throw new MemoryException("could not allocate memory");
		}

		return ret;
	}

	/**
	 * Frees a memory block
	 * @param p_address
	 *            the address of the block
	 * @throws MemoryException If free'ing memory in segment failed.
	 */
	public void free(final long p_address) throws MemoryException {
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

		assertSegmentBounds(p_address);
		
		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - 1));
		blockSize = getSize(p_address);

		freeSize = blockSize + lengthFieldSize;
		address = p_address;

		// Only merge if left neighbor within same segment
		if (address - 1 != m_base) {
			// Read left part of the marker on the left
			leftMarker = readLeftPartOfMarker(address - 1);
			leftFree = true;
			switch (leftMarker) {
			case 0:
				// Left neighbor block (<= 12 byte) is free -> merge free blocks
				leftSize = read(address - 2, 1);
				// merge marker byte
				leftSize += 1;
				break;
			case 1:
			case 2:
			case 3:
			case 4:
			case 5:
				// Left neighbor block is free -> merge free blocks
				leftSize = read(address - 1 - leftMarker, leftMarker);
				// skip leftSize and marker byte from address to get block offset
				unhookFreeBlock(address - leftSize - 1);
				leftSize += 1; // we also merge the marker byte
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
		if (address + blockSize + 1 != m_base + m_size) {
			// Read right part of the marker on the right
			rightMarker = readRightPartOfMarker(p_address + lengthFieldSize + blockSize);
			rightFree = true;
			switch (rightMarker) {
			case 0:
				// Right neighbor block (<= 12 byte) is free -> merge free blocks
				// + 1 to skip marker byte
				rightSize = getSize(p_address + lengthFieldSize + blockSize + 1);
				// merge marker byte
				rightSize += 1;
				break;
			case 1:
			case 2:
			case 3:
			case 4:
			case 5:
				// Right neighbor block is free -> merge free blocks
				// + 1 to skip marker byte
				rightSize = getSize(p_address + lengthFieldSize + blockSize + 1);				
				unhookFreeBlock(p_address + lengthFieldSize + blockSize + 1);
				rightSize += 1; // we also merge the marker byte
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
				m_status.m_smallBlocks++;
			}
		} else if (leftFree && !rightFree) {
			m_status.m_freeSpace += blockSize + lengthFieldSize + 1;
			if (blockSize + lengthFieldSize + leftSize >= SMALL_BLOCK_SIZE && leftSize < SMALL_BLOCK_SIZE) {
				m_status.m_smallBlocks--;
			}
		} else if (!leftFree && rightFree) {
			m_status.m_freeSpace += blockSize + lengthFieldSize + 1;
			if (blockSize + lengthFieldSize + rightSize >= SMALL_BLOCK_SIZE && rightSize < SMALL_BLOCK_SIZE) {
				m_status.m_smallBlocks--;
			}
		} else if (leftFree && rightFree) {
			// +2 for two marker bytes being merged
			m_status.m_freeSpace += blockSize + lengthFieldSize + 2;
			m_status.m_freeBlocks--;
			m_status.m_smallBlocks--;
			if (blockSize + lengthFieldSize + leftSize + rightSize >= SMALL_BLOCK_SIZE) {
				if (rightSize < SMALL_BLOCK_SIZE && leftSize < SMALL_BLOCK_SIZE) {
					m_status.m_smallBlocks--;
				} else if (rightSize >= SMALL_BLOCK_SIZE && leftSize >= SMALL_BLOCK_SIZE) {
					m_status.m_smallBlocks++;
				}
			}
		}

		MemoryStatistic.getInstance().free(blockSize + lengthFieldSize);
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
	public void set(final long p_address, final long p_size, final byte p_value) throws MemoryException {
		assertSegmentBounds(p_address, p_size);
		assertSegmentMaxBlocksize(p_size);
		
		try {
			int lengthFieldSize;

			// skip length byte(s)
			lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - 1));

			m_memory.set(p_address + lengthFieldSize, p_size, p_value);
		} catch (final Throwable e) {
			throw new MemoryException("Could not set memory, addr " + p_address + 
					", size " + p_size + ", value " + p_value + " of segment: " + this);
		} 
	}

	/**
	 * Read a single byte from the specified address.
	 * @param p_address
	 *            Address.
	 * @return Byte read.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public byte readByte(final long p_address) throws MemoryException {
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
	public byte readByte(final long p_address, final long p_offset) throws MemoryException {
		assertSegmentBounds(p_address, p_offset);
		
		try {
			int lengthFieldSize;
			int size;

			// skip length byte(s)
			lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - 1));
			size = (int) read(p_address, lengthFieldSize);

			Contract.check(p_offset < size, "Offset out of bounds");
			Contract.check(size >= Byte.BYTES && p_offset + Byte.BYTES <= size, "Byte read exceeds bounds");

			return m_memory.readByte(p_address + lengthFieldSize + p_offset);
		} catch (final Throwable e) {
			throw new MemoryException("Could not access memory", e);
		}
	}

	/**
	 * Read a single short from the specified address.
	 * @param p_address
	 *            Address
	 * @return Short read.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public short readShort(final long p_address) throws MemoryException {
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
	public short readShort(final long p_address, final long p_offset) throws MemoryException {
		assertSegmentBounds(p_address, p_offset);
		
		try {
			int lengthFieldSize;
			int size;

			// skip length byte(s)
			lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - 1));
			size = (int) read(p_address, lengthFieldSize);

			Contract.check(p_offset < size, "Offset out of bounds");
			Contract.check(size >= Short.BYTES && p_offset + Short.BYTES <= size, "Short read exceeds bounds");

			return m_memory.readShort(p_address + lengthFieldSize + p_offset);
		} catch (final Throwable e) {
			throw new MemoryException("Could not access memory", e);
		}
	}

	/**
	 * Read a single int from the specified address.
	 * @param p_address
	 *            Address.
	 * @return Int read.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public int readInt(final long p_address) throws MemoryException {
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
	public int readInt(final long p_address, final long p_offset) throws MemoryException {
		assertSegmentBounds(p_address, p_offset);
		
		try {
			int lengthFieldSize;
			int size;

			// skip length byte(s)
			lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - 1));
			size = (int) read(p_address, lengthFieldSize);

			Contract.check(p_offset < size, "Offset out of bounds");
			Contract.check(size >= Integer.BYTES && p_offset + Integer.BYTES <= size, "Int read exceeds bounds");

			return m_memory.readInt(p_address + lengthFieldSize + p_offset);
		} catch (final Throwable e) {
			throw new MemoryException("Could not access memory", e);
		}
	}

	/**
	 * Read a long from the specified address.
	 * @param p_address
	 *            Address.
	 * @return Long read.
	 * @throws MemoryException
	 *             If accessing memory failed.
	 */
	public long readLong(final long p_address) throws MemoryException {
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
	public long readLong(final long p_address, final long p_offset) throws MemoryException {
		assertSegmentBounds(p_address, p_offset);
		
		try {
			int lengthFieldSize;
			int size;

			// skip length byte(s)
			lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - 1));
			size = (int) read(p_address, lengthFieldSize);

			Contract.check(p_offset < size, "Offset out of bounds");
			Contract.check(size >= Long.BYTES || p_offset + Long.BYTES <= size, "Long read exceeds bounds");

			return m_memory.readLong(p_address + lengthFieldSize + p_offset);
		} catch (final Throwable e) {
			throw new MemoryException("Could not access memory", e);
		}
	}

	/**
	 * Reads a block of bytes from the memory
	 * @param p_address
	 *            the address to start reading
	 * @return the read bytes
	 * @throws MemoryException
	 *             if the bytes could not be read
	 */
	public byte[] readBytes(final long p_address) throws MemoryException {
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
	public byte[] readBytes(final long p_address, final long p_offset) throws MemoryException {
		assertSegmentBounds(p_address, p_offset);
		
		byte[] ret;
		int lengthFieldSize;
		int size;
		long offset;

		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - 1));
		size = (int) read(p_address, lengthFieldSize);

		Contract.check(p_offset < size, "Offset out of bounds");

		try {
			ret = new byte[(int) (size - p_offset)];

			offset = p_address + lengthFieldSize + p_offset;
			for (int i = 0; i < size - p_offset; i++) {
				ret[i] = m_memory.readByte(offset + i);
			}
		} catch (final Throwable e) {
			throw new MemoryException("Could not access memory", e);
		}

		return ret;
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
	public void writeByte(final long p_address, final byte p_value) throws MemoryException {
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
	public void writeByte(final long p_address, final long p_offset, final byte p_value)
			throws MemoryException {
		assertSegmentBounds(p_address, p_offset);
		
		try {
			int lengthFieldSize;
			int size;

			// skip length byte(s)
			lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - 1));
			size = (int) read(p_address, lengthFieldSize);

			Contract.check(p_offset < size, "Offset out of bounds");
			Contract.check(size >= Byte.BYTES && p_offset + Byte.BYTES <= size, "Byte won't fit into allocated memory");

			m_memory.writeByte(p_address + lengthFieldSize + p_offset, p_value);
		} catch (final Throwable e) {
			throw new MemoryException("Could not access memory", e);
		}
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
	public void writeShort(final long p_address, final short p_value) throws MemoryException {
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
	public void writeShort(final long p_address, final long p_offset, final short p_value)
			throws MemoryException {
		assertSegmentBounds(p_address, p_offset);
		
		try {
			int lengthFieldSize;
			int size;

			// skip length byte(s)
			lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - 1));
			size = (int) read(p_address, lengthFieldSize);

			Contract.check(p_offset < size, "Offset out of bounds");
			Contract.check(size >= Short.BYTES && p_offset + Short.BYTES <= size,
					"Short won't fit into allocated memory");

			m_memory.writeShort(p_address + lengthFieldSize + p_offset, p_value);
		} catch (final Throwable e) {
			throw new MemoryException("Could not access memory", e);
		}
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
	public void writeInt(final long p_address, final int p_value) throws MemoryException {
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
	public void writeInt(final long p_address, final long p_offset, final int p_value)
			throws MemoryException {
		assertSegmentBounds(p_address, p_offset);
		
		try {
			int lengthFieldSize;
			int size;

			// skip length byte(s)
			lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - 1));
			size = (int) read(p_address, lengthFieldSize);

			Contract.check(p_offset < size, "Offset out of bounds");
			Contract.check(size >= Integer.BYTES && p_offset + Integer.BYTES <= size,
					"Int won't fit into allocated memory");

			m_memory.writeInt(p_address + lengthFieldSize + p_offset, p_value);
		} catch (final Throwable e) {
			throw new MemoryException("Could not access memory", e);
		}
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
	public void writeLong(final long p_address, final long p_value) throws MemoryException {
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
	public void writeLong(final long p_address, final long p_offset, final long p_value)
			throws MemoryException {
		assertSegmentBounds(p_address, p_offset);
		
		try {
			int lengthFieldSize;
			int size;

			// skip length byte(s)
			lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - 1));
			size = (int) read(p_address, lengthFieldSize);

			Contract.check(p_offset < size, "Offset out of bounds");
			Contract.check(size >= Long.BYTES && p_offset + Long.BYTES <= size, "Long won't fit into allocated memory");

			m_memory.writeLong(p_address + lengthFieldSize + p_offset, p_value);
		} catch (final Throwable e) {
			throw new MemoryException("Could not access memory", e);
		}
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
	public void writeBytes(final long p_address, final byte[] p_value) throws MemoryException {
		writeBytes(p_address, 0, p_value);
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
	public void writeBytes(final long p_address, final long p_offset, final byte[] p_value)
			throws MemoryException {
		writeBytes(p_address, p_offset, p_value, p_value.length);
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
	public void writeBytes(final long p_address, final long p_offset, final byte[] p_value, final int p_length)
			throws MemoryException {
		assertSegmentBounds(p_address, p_offset);
		
		int lengthFieldSize;
		int size;
		long offset;

		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - 1));
		size = (int) read(p_address, lengthFieldSize);

		Contract.check(p_offset < size, "Offset out of bounds");
		Contract.check(p_offset + p_length <= size, "Array won't fit memory");

		try {
			offset = p_address + lengthFieldSize + p_offset;
			for (int i = 0; i < p_length; i++) {
				m_memory.writeByte(offset + i, p_value[i]);
			}
		} catch (final Throwable e) {
			throw new MemoryException("Could not access memory", e);
		}
	}

	/**
	 * Get the user definable state of a specified address referring
	 * a malloc'd block of memory.
	 * @param p_address
	 *            Address of malloc'd block of memory.
	 * @return User definable state stored for that block (valid values: 0, 1, 2. invalid: -1)
	 * @throws MemoryException If reading memory fails.
	 */
	public int getCustomState(final long p_address) throws MemoryException {
		int marker;
		int ret;
		
		assertSegmentBounds(p_address);

		marker = readRightPartOfMarker(p_address - 1);
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
	public void setCustomState(final long p_address, final int p_customState) throws MemoryException {
		int marker;
		int lengthFieldSize;
		int size;
		
		assertSegmentBounds(p_address);

		Contract.check(p_customState >= 0 && p_customState < 3, 
				"Custom state (" + p_customState + ") out of range, addr: " + p_address);

		marker = readRightPartOfMarker(p_address - 1);
		Contract.check(marker != SINGLE_BYTE_MARKER, 
				"Single byte marker not valid, addr " + p_address + ", marker: " + marker);
		Contract.check(marker > OCCUPIED_FLAGS_OFFSET,
				"Invalid marker " + marker + " at address " + p_address);

		lengthFieldSize = getSizeFromMarker(marker);
		size = (int) read(p_address, lengthFieldSize);
		marker = (OCCUPIED_FLAGS_OFFSET + lengthFieldSize) + (p_customState * 3);

		writeRightPartOfMarker(p_address - 1, marker);
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
	public int getSize(final long p_address) throws MemoryException {
		int lengthFieldSize;
		
		assertSegmentBounds(p_address);

		lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - 1));
		return (int) read(p_address, lengthFieldSize);
	}
	
	@Override
	public String toString() {
		return "Segment(id: " + m_segmentID + ", m_assignedThread " + m_assignedThread + "): " + 
				"m_base " + m_base + ", " +
				"m_baseFreeBlockList " + m_baseFreeBlockList + ", " +
				"m_size " + m_size + ", " +
				"m_fullSize " + m_fullSize + ", " +
				"segment borders: " + m_base + "|" + (m_base + m_fullSize) + ", " +
				"status: " + m_status;
	}
	
	/**
	 * Calculates the required memory for multiple objects
	 * @param p_sizes
	 *            the sizes od the objects
	 * @return the size of the required memory
	 */
	static public int getRequiredMemory(final int... p_sizes) {
		int ret;

		Contract.checkNotNull(p_sizes, "no sizes given");
		Contract.check(p_sizes.length > 0, "no sizes given");

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

	private void assertSegmentBounds(final long p_address) throws MemoryException
	{
		if (p_address < m_base || p_address > m_base + m_fullSize)
			throw new MemoryException("Address " + p_address + " is not within segment: " + this);
	}
	
	private void assertSegmentBounds(final long p_address, final long p_length) throws MemoryException
	{
		if (p_address < m_base || p_address > m_base + m_fullSize || 
			p_address + p_length < m_base || p_address + p_length > m_base + m_fullSize)
			throw new MemoryException("Address " + p_address + 
					" with length " + p_length + "is not within segment: " + this);
	}
	
	public void assertSegmentMaxBlocksize(final long p_size) throws MemoryException
	{
		if (p_size > MAX_SIZE_MEMORY_BLOCK)
			throw new MemoryException("Size " + p_size + 
					" exceeds max blocksize " + MAX_SIZE_MEMORY_BLOCK + " of segment: " + this);
	}
	
	/**
	 * Creates a free block
	 * @param p_address
	 *            the address
	 * @param p_size
	 *            the size
	 * @throws MemoryException If creating free block failed.
	 */
	private void createFreeBlock(final long p_address, final long p_size) throws MemoryException {
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
			writePointer(anchor + readRightPartOfMarker(anchor - 1), p_address);
		}
		// Write pointer of list
		writePointer(listOffset, p_address);

		// Write length
		write(p_address, p_size, lengthFieldSize);
		write(p_address + p_size - lengthFieldSize, p_size, lengthFieldSize);
		}

		// Write right and left marker
		writeRightPartOfMarker(p_address - 1, lengthFieldSize);
		writeLeftPartOfMarker(p_address + p_size, lengthFieldSize);
	}

	/**
	 * Unhooks a free block
	 * @param p_address
	 *            the address
	 * @throws MemoryException If unhooking free block failed.
	 */
	private void unhookFreeBlock(final long p_address) throws MemoryException {
		int lengthFieldSize;
		long prevPointer;
		long nextPointer;

		// Read size of length field
		lengthFieldSize = readRightPartOfMarker(p_address - 1);

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
	private int readRightPartOfMarker(final long p_address) throws MemoryException {
		return m_memory.readByte(p_address) & 0xF;
	}

	/**
	 * Read the left part of a marker byte
	 * @param p_address
	 *            the address
	 * @return the left part of a marker byte
	 * @throws MemoryException If reading fails.
	 */
	private int readLeftPartOfMarker(final long p_address) throws MemoryException {
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
	private void writeRightPartOfMarker(final long p_address, final int p_right) throws MemoryException {
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
	private void writeLeftPartOfMarker(final long p_address, final int p_left) throws MemoryException {
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
	private long readPointer(final long p_address) throws MemoryException {
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
	private void writePointer(final long p_address, final long p_pointer) throws MemoryException {
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
	private long read(final long p_address, final int p_count) throws MemoryException {
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
	private void write(final long p_address, final long p_bytes, final int p_count) throws MemoryException {
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
		private int m_smallBlocks;
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
			m_smallBlocks = 0;
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
			return m_smallBlocks;
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
			ret.m_smallBlocks = m_smallBlocks;

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
			return "Status [m_freeSpace=" + m_freeSpace + ", m_freeBlocks=" + m_freeBlocks + ", m_smallBlocks=" + m_smallBlocks + "]";
		}

	}

}