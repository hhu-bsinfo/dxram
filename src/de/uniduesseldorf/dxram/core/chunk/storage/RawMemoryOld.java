package de.uniduesseldorf.dxram.core.chunk.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;

import sun.misc.Unsafe;

import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.utils.Contract;
import de.uniduesseldorf.dxram.utils.Tools;
import de.uniduesseldorf.dxram.utils.locks.SpinLock;
import de.uniduesseldorf.dxram.utils.unsafe.UnsafeHandler;

/**
 * Manages a large memory block
 * @author Florian Klein 13.02.2014
 */
public final class RawMemoryOld {

	// Constants
	private static final byte POINTER_SIZE = 5;
	private static final byte SMALL_BLOCK_SIZE = 64;
	private static final byte BITMASK_LENGTH_FIELD_SIZE = 0x7;
	private static final byte OCCUPIED_FLAG = 0x8;
	private static final byte SINGLE_BYTE_MARKER = 0xF;

	private static final byte LIST_COUNT = 29;
	private static final int LIST_SIZE = LIST_COUNT * POINTER_SIZE + 3;
	private static final long SEGMENT_SIZE = 1 << LIST_COUNT + 1;
	private static final long FULL_SEGMENT_SIZE = SEGMENT_SIZE + 2 + LIST_SIZE;

	private static final int MAX_REQUEST_WAIT_TIME = 100;

	private static final Unsafe UNSAFE = UnsafeHandler.getInstance().getUnsafe();

	// Attributes
	private static long m_memoryBase;
	private static long m_memorySize;

	private static Segment[] m_segments;
	private static ArenaManager m_arenaManager;

	private static long[] m_listSizes;

	// Constructors
	/**
	 * Creates an instance of RawMemory
	 */
	private RawMemoryOld() {}

	// Getters
	/**
	 * Return the current base address
	 * @return the memoryBase the base address
	 */
	protected static long getMemoryBase() {
		return m_memoryBase;
	}

	// Methods
	/**
	 * Initializes the memory
	 * @param p_size
	 *            the size of the memory
	 * @return the actual size of the memory
	 * @throws MemoryException
	 *             if the memory could not be initialized
	 */
	public static long initialize(final long p_size) throws MemoryException {
		long ret;
		int segmentCount;
		long base;
		long remaining;
		long size;

		Contract.check(p_size >= 0, "invalid size given");

		segmentCount = (int)(p_size / SEGMENT_SIZE);
		if (p_size % SEGMENT_SIZE > 0) {
			segmentCount++;
		}

		// Initializes the list sizes
		m_listSizes = new long[LIST_COUNT];
		for (int i = 0;i < LIST_COUNT;i++) {
			m_listSizes[i] = (long)Math.pow(2, i + 2);
		}
		m_listSizes[0] = 12;
		m_listSizes[1] = 24;
		m_listSizes[2] = 36;
		m_listSizes[3] = 48;

		// Set the size of the memory
		m_memorySize = p_size + segmentCount * (2 + LIST_SIZE);

		// Allocate Memory
		try {
			m_memoryBase = UNSAFE.allocateMemory(m_memorySize);
			UNSAFE.setMemory(m_memoryBase, m_memorySize, (byte)0);
		} catch (final Throwable e) {
			throw new MemoryException("Could not initialize memory", e);
		}

		// Initialize segments
		base = 0;
		remaining = p_size;
		m_segments = new Segment[segmentCount];
		for (int i = 0;i < segmentCount;i++) {
			size = Math.min(SEGMENT_SIZE, remaining);
			m_segments[i] = new Segment(base, size);

			base += FULL_SEGMENT_SIZE;
			remaining -= SEGMENT_SIZE;
		}
		m_arenaManager = new ArenaManager();

		System.out.println("RawMemory: init success (size=" + Tools.readableSize(m_memorySize) + ", base-addr=0x"
				+ Long.toHexString(m_memoryBase) + ")");

		ret = m_memorySize;

		Timer t = new Timer(true);
		t.schedule(new TimerTask() {

			@Override
			public void run() {
				printDebugInfos();
			}

		}, 0, 60000);

		return ret;
	}

	/**
	 * Disengages the memory
	 * @throws MemoryException
	 *             if the memory could not be disengaged
	 */
	public static void disengage() throws MemoryException {
		try {
			UNSAFE.freeMemory(m_memoryBase);

			m_memorySize = 0;
			m_memoryBase = 0;

			m_segments = null;
			m_arenaManager = null;

			m_listSizes = null;
		} catch (final Throwable e) {
			throw new MemoryException("Could not free memory", e);
		}
	}

	/**
	 * Allocate a memory block
	 * @param p_size
	 *            the size of the block
	 * @return the offset of the block
	 * @throws MemoryException
	 *             if the memory block could not be allocated
	 */
	public static long malloc(final int p_size) throws MemoryException {
		long ret = -1;
		Segment segment;
		long threadID;

		threadID = Thread.currentThread().getId();

		segment = m_arenaManager.getArena(threadID);

		// Try to allocate in the current segment
		ret = segment.malloc(p_size);

		// Try to allocate in another segment
		if (ret == -1) {
			segment = m_arenaManager.assignNewSegment(threadID, segment);

			// Try to allocate in the current segment
			ret = segment.malloc(p_size);
		}

		if (ret == -1) {
			ret = m_arenaManager.delegateMalloc(p_size);
		}

		if (ret == -1) {
			throw new MemoryException("could not allocate memory");
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
	protected static long[] malloc(final int... p_sizes) throws MemoryException {
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

			for (int i = 0;i < p_sizes.length;i++) {
				if (p_sizes[i] > 0) {
					if (p_sizes[i] >= 1 << 16) {
						lengthfieldSize = 3;
					} else if (p_sizes[i] >= 1 << 8) {
						lengthfieldSize = 2;
					} else {
						lengthfieldSize = 1;
					}
					marker = (byte)(OCCUPIED_FLAG + lengthfieldSize);

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
	 * @throws MemoryException
	 *             if the block could not be freed
	 */
	protected static void free(final long p_address) throws MemoryException {
		Segment segment;
		long threadID;

		threadID = Thread.currentThread().getId();

		segment = m_arenaManager.getArena(threadID);

		if (segment.equals(m_segments[(int)(p_address / FULL_SEGMENT_SIZE)])) {
			try {
				segment.free(p_address);
			} catch (final Throwable e) {
				throw new MemoryException("could not free memory");
			}
		} else {
			m_arenaManager.delegateFree(p_address);
		}

	}

	/**
	 * Frees multiple memory blocks
	 * @param p_addresses
	 *            the addresses of the blocks
	 * @throws MemoryException
	 *             if the blocks could not be freed
	 */
	protected static void free(final long... p_addresses) throws MemoryException {
		boolean exceptionOccured = false;

		Contract.checkNotNull(p_addresses, "no addresses given");

		for (final long address : p_addresses) {
			if (address != 0) {
				try {
					free(address);
				} catch (final MemoryException e) {
					exceptionOccured = true;
				}
			}
		}

		if (exceptionOccured) {
			throw new MemoryException("could not free memory");
		}
	}

	/**
	 * Calculates the required memory for multiple objects
	 * @param p_sizes
	 *            the sizes od the objects
	 * @return the size of the required memory
	 */
	private static int getRequiredMemory(final int... p_sizes) {
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
	protected static void set(final long p_address, final long p_size, final byte p_value) throws MemoryException {
		try {
			UNSAFE.setMemory(m_memoryBase + p_address, p_size, p_value);
		} catch (final Throwable e) {
			throw new MemoryException("Could not free memory", e);
		}
	}

	/**
	 * Reads a byte from the memory
	 * @param p_address
	 *            the address to read
	 * @return the read byte
	 * @throws DXRAMException
	 *             if the byte could not be read
	 */
	protected static byte readByte(final long p_address) throws DXRAMException {
		try {
			return UNSAFE.getByte(m_memoryBase + p_address);
		} catch (final Throwable e) {
			throw new MemoryException("Could not access memory", e);
		}
	}

	/**
	 * Reads an int from the memory
	 * @param p_address
	 *            the address to read
	 * @return the read int
	 * @throws MemoryException
	 *             if the int could not be read
	 */
	protected static int readInt(final long p_address) throws MemoryException {
		try {
			return UNSAFE.getInt(m_memoryBase + p_address);
		} catch (final Throwable e) {
			throw new MemoryException("Could not access memory", e);
		}
	}

	/**
	 * Reads a long from the memory
	 * @param p_address
	 *            the address to read
	 * @return the read long
	 * @throws MemoryException
	 *             if the long could not be read
	 */
	protected static long readLong(final long p_address) throws MemoryException {
		try {
			return UNSAFE.getLong(m_memoryBase + p_address);
		} catch (final Throwable e) {
			throw new MemoryException("Could not access memory", e);
		}
	}

	/**
	 * Reads a block of bytes from the memory
	 * @param p_address
	 *            the address to start reading
	 * @return the read byte
	 * @throws MemoryException
	 *             if the bytes could not be read
	 */
	protected static byte[] readBytes(final long p_address) throws MemoryException {
		byte[] ret;
		int lengthFieldSize;
		int size;
		long offset;

		lengthFieldSize = readRightPartOfMarker(p_address - 1) & BITMASK_LENGTH_FIELD_SIZE;
		size = (int)read(p_address, lengthFieldSize);

		try {
			ret = new byte[size];

			offset = m_memoryBase + p_address + lengthFieldSize;
			for (int i = 0;i < size;i++) {
				ret[i] = UNSAFE.getByte(offset + i);
			}
		} catch (final Throwable e) {
			throw new MemoryException("Could not access memory", e);
		}

		return ret;
	}

	/**
	 * Writes a byte to the memory
	 * @param p_address
	 *            the address to write
	 * @param p_value
	 *            the byte to write
	 * @throws MemoryException
	 *             if the byte could not be written
	 */
	protected static void writeByte(final long p_address, final byte p_value) throws MemoryException {
		try {
			UNSAFE.putByte(m_memoryBase + p_address, p_value);
		} catch (final Throwable e) {
			throw new MemoryException("Could not access memory", e);
		}
	}

	/**
	 * Writes an int to the memory
	 * @param p_address
	 *            the address to write
	 * @param p_value
	 *            the int to write
	 * @throws MemoryException
	 *             if the int could not be written
	 */
	protected static void writeInt(final long p_address, final int p_value) throws MemoryException {
		try {
			UNSAFE.putInt(m_memoryBase + p_address, p_value);
		} catch (final Throwable e) {
			throw new MemoryException("Could not access memory", e);
		}
	}

	/**
	 * Writes a long to the memory
	 * @param p_address
	 *            the address to write
	 * @param p_value
	 *            the long to write
	 * @throws MemoryException
	 *             if the long could not be written
	 */
	protected static void writeLong(final long p_address, final long p_value) throws MemoryException {
		try {
			UNSAFE.putLong(m_memoryBase + p_address, p_value);
		} catch (final Throwable e) {
			throw new MemoryException("Could not access memory", e);
		}
	}

	/**
	 * Writes a number of bytes to the memory
	 * @param p_address
	 *            the address to write
	 * @param p_value
	 *            the bytes to write
	 * @throws MemoryException
	 *             if the bytes could not be written
	 */
	protected static void writeBytes(final long p_address, final byte[] p_value) throws MemoryException {
		int lengthFieldSize;
		int size;
		long offset;

		lengthFieldSize = readRightPartOfMarker(p_address - 1) & BITMASK_LENGTH_FIELD_SIZE;
		size = (int)read(p_address, lengthFieldSize);

		Contract.check(p_value.length == size, "array size differs from memory size");

		try {
			offset = m_memoryBase + p_address + lengthFieldSize;
			for (int i = 0;i < p_value.length;i++) {
				UNSAFE.putByte(offset + i, p_value[i]);
			}
		} catch (final Throwable e) {
			throw new MemoryException("Could not access memory", e);
		}
	}

	/**
	 * Gets the current fragmentation of all segments
	 * @return the fragmentation
	 */
	protected static double[] getFragmentation() {
		double[] ret;

		ret = new double[m_segments.length];
		for (int i = 0;i < m_segments.length;i++) {
			ret[i] = m_segments[i].getFragmentation();
		}

		return ret;
	}

	/**
	 * Gets the segment for the given address
	 * @param p_address
	 *            the address
	 * @return the segment
	 */
	protected static int getSegment(final long p_address) {
		return (int)(p_address / FULL_SEGMENT_SIZE);
	}

	/**
	 * Gets the current segment status
	 * @return the segment status
	 */
	public static Segment.Status[] getSegmentStatus() {
		Segment.Status[] ret;

		ret = new Segment.Status[m_segments.length];
		for (int i = 0;i < m_segments.length;i++) {
			ret[i] = m_segments[i].getStatus();
		}

		return ret;
	}

	public static void printDebugInfos() {
		StringBuilder output;
		Segment.Status[] stati;
		long freeSpace;
		long freeBlocks;

		stati = getSegmentStatus();
		freeSpace = 0;
		freeBlocks = 0;
		for (Segment.Status status : stati) {
			freeSpace += status.m_freeSpace;
			freeBlocks += status.m_freeBlocks;
		}

		output = new StringBuilder();
		output.append("\nRawMemory (" + Long.toHexString(m_memoryBase).toUpperCase() + "):");
		output.append("\nSize: " + Tools.readableSize(m_memorySize));
		output.append("\nSegment Count: " + m_segments.length + " at " + Tools.readableSize(FULL_SEGMENT_SIZE));
		output.append("\nFree Space: " + Tools.readableSize(freeSpace) + " in " + freeBlocks + " blocks");
		for (int i = 0;i < stati.length;i++) {
			output.append("\n\tSegment " + i + " (" + m_segments[i].m_assigned + "): "
					+ Tools.readableSize(stati[i].m_freeSpace) + " in " + stati[i].m_freeBlocks + " blocks");
		}
		output.append("\n");

		System.out.println(output);
	}

	/**
	 * Gets the suitable list for the given size
	 * @param p_size
	 *            the size
	 * @return the suitable list
	 */
	private static int getList(final long p_size) {
		int ret = 0;

		while (ret + 1 < m_listSizes.length && m_listSizes[ret + 1] <= p_size) {
			ret++;
		}

		return ret;
	}

	/**
	 * Read the right part of a marker byte
	 * @param p_address
	 *            the address
	 * @return the right part of a marker byte
	 */
	private static int readRightPartOfMarker(final long p_address) {
		return UNSAFE.getByte(m_memoryBase + p_address) & 0xF;
	}

	/**
	 * Read the left part of a marker byte
	 * @param p_address
	 *            the address
	 * @return the left part of a marker byte
	 */
	private static int readLeftPartOfMarker(final long p_address) {
		return (UNSAFE.getByte(m_memoryBase + p_address) & 0xF0) >> 4;
	}

	/**
	 * Writes a marker byte
	 * @param p_address
	 *            the address
	 * @param p_right
	 *            the right part
	 */
	private static void writeRightPartOfMarker(final long p_address, final int p_right) {
		byte marker;

		marker = (byte)((UNSAFE.getByte(m_memoryBase + p_address) & 0xF0) + (p_right & 0xF));
		UNSAFE.putByte(m_memoryBase + p_address, marker);
	}

	/**
	 * Writes a marker byte
	 * @param p_address
	 *            the address
	 * @param p_left
	 *            the left part
	 */
	private static void writeLeftPartOfMarker(final long p_address, final int p_left) {
		byte marker;

		marker = (byte)(((p_left & 0xF) << 4) + (UNSAFE.getByte(m_memoryBase + p_address) & 0xF));
		UNSAFE.putByte(m_memoryBase + p_address, marker);
	}

	/**
	 * Reads a pointer
	 * @param p_address
	 *            the address
	 * @return the pointer
	 */
	private static long readPointer(final long p_address) {
		return read(p_address, POINTER_SIZE);
	}

	/**
	 * Writes a pointer
	 * @param p_address
	 *            the address
	 * @param p_pointer
	 *            the pointer
	 */
	private static void writePointer(final long p_address, final long p_pointer) {
		write(p_address, p_pointer, POINTER_SIZE);
	}

	/**
	 * Reads up to 8 bytes combined in a long
	 * @param p_address
	 *            the address
	 * @param p_count
	 *            the number of bytes
	 * @return the combined bytes
	 */
	private static long read(final long p_address, final int p_count) {
		long ret = 0;
		long bitmask;

		bitmask = 0xFFFFFFFFFFFFFFFFL >>> (8 - p_count) * 8;

		ret = UNSAFE.getLong(m_memoryBase + p_address);
		ret = ret & bitmask;

		return ret;
	}

	/**
	 * Writes up to 8 bytes combined in a long
	 * @param p_address
	 *            the address
	 * @param p_bytes
	 *            the combined bytes
	 * @param p_count
	 *            the number of bytes
	 */
	private static void write(final long p_address, final long p_bytes, final int p_count) {
		long value;
		long bitmask;

		value = p_bytes;

		if (p_count < 8) {
			bitmask = 0xFFFFFFFFFFFFFFFFL << p_count * 8;

			// Read current value
			value = UNSAFE.getLong(m_memoryBase + p_address);
			value = value & bitmask;

			bitmask = 0xFFFFFFFFFFFFFFFFL >>> (8 - p_count) * 8;

			value += p_bytes & bitmask;
		}

		UNSAFE.putLong(m_memoryBase + p_address, value);
	}

	// Classes
	/**
	 * Represents a segment of the memory
	 * @author Florian Klein 04.04.2014
	 */
	public static final class Segment {

		// Attributes
		private long m_pointerOffset;
		private Status m_status;
		private boolean m_assigned;

		// Constructors
		/**
		 * Creates an instance of Segment
		 * @param p_base
		 *            the base address of the segment
		 * @param p_size
		 *            the size of the segment
		 */
		private Segment(final long p_base, final long p_size) {
			m_pointerOffset = p_base + p_size + 2;
			m_status = new Status(p_size);
			m_assigned = false;

			// Create a free block in the complete memory
			createFreeBlock(p_base + 1, p_size);
			writeLeftPartOfMarker(p_base, OCCUPIED_FLAG);
			writeRightPartOfMarker(p_base + p_size, OCCUPIED_FLAG);
		}

		// Getters
		/**
		 * Gets the status
		 * @return the status
		 */
		private Status getStatus() {
			return m_status.copy(m_pointerOffset);
		}

		/**
		 * Checks if the Segment is assigned
		 * @return true if the Segment is assigned, false otherwise
		 */
		private boolean isAssigned() {
			return m_assigned;
		}

		// Methods
		/**
		 * Assigns the Segment
		 */
		private void assign() {
			m_assigned = true;
		}

		/**
		 * Unassigns the Segment
		 */
		private void unassign() {
			m_assigned = false;
		}

		/**
		 * Gets the current fragmentation
		 * @return the fragmentation
		 */
		private double getFragmentation() {
			double ret = 0;
			int free;
			int small;

			free = m_status.getFreeBlocks();
			small = m_status.getSmallBlocks();

			if (small >= 1 || free >= 1) {
				ret = (double)small / free;
			}

			return ret;
		}

		/**
		 * Allocate a memory block
		 * @param p_size
		 *            the size of the block
		 * @return the offset of the block
		 */
		private long malloc(final int p_size) {
			long ret = -1;
			int list;
			long address;
			long size;
			int lengthFieldSize;
			long freeSize;
			int freeLengthFieldSize;
			byte marker;

			if (p_size >= 1 << 16) {
				lengthFieldSize = 3;
			} else if (p_size >= 1 << 8) {
				lengthFieldSize = 2;
			} else {
				lengthFieldSize = 1;
			}
			size = p_size + lengthFieldSize;

			marker = (byte)(OCCUPIED_FLAG + lengthFieldSize);

			// Get the list with a free block which is big enough
			list = getList(size) + 1;
			while (list < LIST_COUNT && readPointer(m_pointerOffset + list * POINTER_SIZE) == 0) {
				list++;
			}
			if (list < LIST_COUNT) {
				// A list is found
				address = readPointer(m_pointerOffset + list * POINTER_SIZE);
			} else {
				// Traverse through the lower list
				list = getList(size);
				address = readPointer(m_pointerOffset + list * POINTER_SIZE);
				if (address != 0) {
					freeLengthFieldSize = readRightPartOfMarker(address - 1);
					freeSize = read(address, freeLengthFieldSize);
					while (freeSize < size && address != 0) {
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
				if (freeSize == size) {
					m_status.m_freeSpace -= size;
					m_status.m_freeBlocks--;
					if (freeSize < SMALL_BLOCK_SIZE) {
						m_status.m_smallBlocks--;
					}
				} else if (freeSize == size + 1) {
					// 1 Byte to big -> write two markers on the right
					writeRightPartOfMarker(address + size, 15);
					writeLeftPartOfMarker(address + size + 1, 15);

					m_status.m_freeSpace -= size + 1;
					m_status.m_freeBlocks--;
					if (freeSize + 1 < SMALL_BLOCK_SIZE) {
						m_status.m_smallBlocks--;
					}
				} else {
					// Block is to big -> create a new free block with the remaining size
					createFreeBlock(address + size + 1, freeSize - size - 1);

					m_status.m_freeSpace -= size + 1;
					if (freeSize >= SMALL_BLOCK_SIZE && freeSize - size - 1 < SMALL_BLOCK_SIZE) {
						m_status.m_smallBlocks++;
					}
				}
				// Write marker
				writeLeftPartOfMarker(address + size, marker);
				writeRightPartOfMarker(address - 1, marker);

				// Write block size
				write(address, p_size, lengthFieldSize);

				ret = address;
			}

			return ret;
		}

		/**
		 * Frees a memory block
		 * @param p_address
		 *            the address of the block
		 */
		private void free(final long p_address) {
			long size;
			int lengthFieldSize;
			long freeSize;
			long address;
			int leftMarker;
			int rightMarker;
			boolean leftFree;
			long leftSize;
			boolean rightFree;
			long rightSize;

			lengthFieldSize = readRightPartOfMarker(p_address - 1) & BITMASK_LENGTH_FIELD_SIZE;
			size = read(p_address, lengthFieldSize) + lengthFieldSize;

			freeSize = size;
			address = p_address;

			// Read left part of the marker on the left
			leftMarker = readLeftPartOfMarker(address - 1);
			leftFree = true;
			switch (leftMarker) {
			case 0:
				// Left neighbor block (<= 12 byte) is free -> merge free blocks
				leftSize = read(address - 2, 1) + 1;
				break;
			case 1:
			case 2:
			case 3:
			case 4:
			case 5:
				// Left neighbor block is free -> merge free blocks
				leftSize = read(address - 1 - leftMarker, leftMarker) + 1;

				unhookFreeBlock(address - leftSize);
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
			address -= leftSize;
			freeSize += leftSize;

			// Read right part of the marker on the right
			rightMarker = readRightPartOfMarker(p_address + size);
			rightFree = true;
			switch (rightMarker) {
			case 0:
				// Right neighbor block (<= 12 byte) is free -> merge free blocks
				rightSize = read(p_address + size + 1, 1) + 1;
				break;
			case 1:
			case 2:
			case 3:
			case 4:
			case 5:
				// Right neighbor block is free -> merge free blocks
				rightSize = read(p_address + size + 1, rightMarker) + 1;

				unhookFreeBlock(p_address + size + 1);
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
			freeSize += rightSize;

			// Create a free block
			createFreeBlock(address, freeSize);

			leftSize--;
			rightSize--;

			if (!leftFree && !rightFree) {
				m_status.m_freeSpace += size;
				m_status.m_freeBlocks++;
				if (size < SMALL_BLOCK_SIZE) {
					m_status.m_smallBlocks++;
				}
			} else if (leftFree && !rightFree) {
				m_status.m_freeSpace += size + 1;
				if (size + leftSize >= SMALL_BLOCK_SIZE && leftSize < SMALL_BLOCK_SIZE) {
					m_status.m_smallBlocks--;
				}
			} else if (!leftFree && rightFree) {
				m_status.m_freeSpace += size + 1;
				if (size + rightSize >= SMALL_BLOCK_SIZE && rightSize < SMALL_BLOCK_SIZE) {
					m_status.m_smallBlocks--;
				}
			} else if (leftFree && rightFree) {
				m_status.m_freeSpace += size + 2;
				m_status.m_freeBlocks--;
				m_status.m_smallBlocks--;
				if (size + leftSize + rightSize >= SMALL_BLOCK_SIZE) {
					if (rightSize < SMALL_BLOCK_SIZE && leftSize < SMALL_BLOCK_SIZE) {
						m_status.m_smallBlocks--;
					} else if (rightSize >= SMALL_BLOCK_SIZE && leftSize >= SMALL_BLOCK_SIZE) {
						m_status.m_smallBlocks++;
					}
				}
			}
		}

		/**
		 * Creates a free block
		 * @param p_address
		 *            the address
		 * @param p_size
		 *            the size
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
				listOffset = m_pointerOffset + getList(p_size) * POINTER_SIZE;

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
		 */
		private void unhookFreeBlock(final long p_address) {
			int lengthFieldSize;
			long prevPointer;
			long nextPointer;

			// Read size of length field
			lengthFieldSize = readRightPartOfMarker(p_address - 1);

			// Read pointers
			prevPointer = readPointer(p_address + lengthFieldSize);
			nextPointer = readPointer(p_address + lengthFieldSize + POINTER_SIZE);

			if (prevPointer >= m_pointerOffset) {
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

		@Override
		public String toString() {
			return "Segment [m_status=" + m_status + ", m_pointerOffset=" + m_pointerOffset + "]";
		}

		// Classes
		/**
		 * Holds fragmentation information of a segment
		 * @author Florian Klein 10.04.2014
		 */
		public static final class Status {

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
			 * @param p_pointerOffset
			 *            the pointer offset
			 * @return the copy
			 */
			private Status copy(final long p_pointerOffset) {
				Status ret;

				ret = new Status(0);
				ret.m_freeSpace = m_freeSpace;
				ret.m_freeBlocks = m_freeBlocks;
				ret.m_smallBlocks = m_smallBlocks;

				ret.m_sizes = m_listSizes;
				// ret.m_blocks = new long[0];
				// ret.m_blocks = getBlocks(p_pointerOffset);

				return ret;
			}

			/**
			 * Gets all free blocks
			 * @param p_pointerOffset
			 *            the pointer offset
			 * @return all free blocks
			 */
			private long[] getBlocks(final long p_pointerOffset) {
				long[] ret;
				long address;
				int lengthFieldSize;
				long count;

				ret = new long[m_listSizes.length];
				for (int i = 0;i < m_listSizes.length;i++) {
					count = 0;

					address = readPointer(p_pointerOffset + i * POINTER_SIZE);
					while (address != 0) {
						count++;

						lengthFieldSize = readRightPartOfMarker(address - 1);
						address = readPointer(address + lengthFieldSize + POINTER_SIZE);
					}

					ret[i] = count;
				}

				return ret;
			}

			@Override
			public String toString() {
				return "Status [m_freeSpace=" + m_freeSpace + ", m_freeBlocks=" + m_freeBlocks + ", m_smallBlocks="
						+ m_smallBlocks + "]";
			}

		}

	}

	/**
	 * Manages the Thread Arenas
	 * @author Florian Klein 28.08.2014
	 */
	private static final class ArenaManager {

		// Attributes
		private Map<Long, Segment> m_arenas;
		private Map<Segment, List<Long>> m_freeRequests;
		private Queue<MallocRequest> m_mallocRequests;

		private Lock m_segmentLock;
		private Lock m_mallocLock;
		private Lock m_freeLock;

		// Constructors
		/**
		 * Creates an instance of ArenaManager
		 */
		private ArenaManager() {
			m_arenas = new HashMap<>();
			m_freeRequests = new HashMap<>();
			m_mallocRequests = new LinkedList<>();

			m_segmentLock = new SpinLock();
			m_mallocLock = new SpinLock();
			m_freeLock = new SpinLock();
		}

		// Methods
		/**
		 * Gets the assigned Segment for the given Thread
		 * @param p_threadID
		 *            the ID of the Thread
		 * @return the assigned Segment
		 */
		private Segment getArena(final long p_threadID) {
			Segment ret;
			List<Long> freeRequests;

			m_segmentLock.lock();
			ret = m_arenas.get(p_threadID);
			m_segmentLock.unlock();
			if (ret == null) {
				System.out.println("New Thread: " + p_threadID);
				ret = assignNewSegment(p_threadID);
			}

			freeRequests = m_freeRequests.get(ret);
			if (freeRequests == null || !freeRequests.isEmpty()) {
				executeFreeRequests(ret, freeRequests);
			}

			if (!m_mallocRequests.isEmpty()) {
				executeMallocRequests(ret);
			}

			return ret;
		}

		/**
		 * Assignes a new Segment to the Thread
		 * @param p_threadID
		 *            the ID of the Thread
		 * @return the new assigned Segment
		 */
		private Segment assignNewSegment(final long p_threadID) {
			return assignNewSegment(p_threadID, null);
		}

		/**
		 * Assignes a new Segment to the Thread
		 * @param p_threadID
		 *            the ID of the Thread
		 * @param p_current
		 *            the current assigned Segment
		 * @return the new assigned Segment
		 * @throws MemoryException
		 *             if no Segment could be assigned
		 */
		private Segment assignNewSegment(final long p_threadID, final Segment p_current) {
			Segment ret = null;
			int index;
			double fragmentation;
			long free;
			double fragmentationTemp;
			long freeTemp;

			m_segmentLock.lock();

			index = -1;
			fragmentation = 1;
			free = 0;
			for (int i = 0;i < m_segments.length;i++) {
				if (!m_segments[i].isAssigned()) {
					fragmentationTemp = m_segments[i].getFragmentation();
					freeTemp = m_segments[i].m_status.getFreeSpace();

					if (fragmentationTemp < fragmentation || fragmentationTemp == fragmentation && freeTemp > free) {
						index = i;
						fragmentation = fragmentationTemp;
						free = freeTemp;
					}
				}
			}

			if (p_current != null) {
				p_current.unassign();
			}

			if (index >= 0) {
				ret = m_segments[index];
				ret.assign();

				m_arenas.put(p_threadID, ret);
			}

			m_segmentLock.unlock();

			return ret;
		}

		/**
		 * Delegates a malloc request to another Thread
		 * @param p_size
		 *            the requested size
		 * @return the requested address
		 */
		private long delegateMalloc(final int p_size) {
			long ret = -1;
			long time;
			MallocRequest request;

			request = new MallocRequest(p_size);

			m_mallocLock.lock();

			m_mallocRequests.offer(request);

			m_mallocLock.unlock();

			time = System.currentTimeMillis();
			do {
				try {
					Thread.sleep(10);
				} catch (final InterruptedException e) {}

				if (request.isExecuted()) {
					ret = request.getAddress();
					break;
				}
			} while (System.currentTimeMillis() - time < MAX_REQUEST_WAIT_TIME);

			return ret;
		}

		/**
		 * Delegates a free request to another Thread
		 * @param p_address
		 *            the requested address
		 */
		private void delegateFree(final long p_address) {
			Segment segment;

			segment = m_segments[(int)(p_address / FULL_SEGMENT_SIZE)];

			m_segmentLock.lock();

			if (!segment.isAssigned()) {
				segment.free(p_address);
			} else {
				m_freeLock.lock();

				m_freeRequests.get(segment).add(p_address);

				m_freeLock.unlock();
			}

			m_segmentLock.unlock();
		}

		/**
		 * Executes pending malloc requests
		 * @param p_segment
		 *            the Segment to execute the requests
		 */
		private void executeMallocRequests(final Segment p_segment) {
			MallocRequest request;
			Iterator<MallocRequest> iterator;
			long address;

			m_mallocLock.lock();

			for (iterator = m_mallocRequests.iterator();iterator.hasNext();) {
				request = iterator.next();

				address = p_segment.malloc(request.getSize());
				if (address >= 0) {
					request.execute(address);

					iterator.remove();
				}
			}

			m_mallocLock.unlock();
		}

		/**
		 * Executes pending free requests
		 * @param p_segment
		 *            the Segment to execute the requests
		 * @param p_freeRequests
		 *            the requests to execute
		 */
		private void executeFreeRequests(final Segment p_segment, final List<Long> p_freeRequests) {
			m_freeLock.lock();

			if (p_freeRequests != null) {
				for (final Long address : p_freeRequests) {
					p_segment.free(address);
				}
			} else {
				m_freeRequests.put(p_segment, new ArrayList<Long>());
			}

			m_freeLock.unlock();
		}

		// Classes
		/**
		 * Represents a request for a malloc operation
		 * @author Florian Klein 29.08.2014
		 */
		private final class MallocRequest {

			// Attributes
			private int m_size;
			private long m_address;

			private boolean m_executed;
			private Lock m_lock;

			// Constructors
			/**
			 * Creates an instance of MallocRequest
			 * @param p_size
			 *            the requested size
			 */
			private MallocRequest(final int p_size) {
				m_size = p_size;
				m_address = -1;

				m_executed = true;
				m_lock = new SpinLock();
			}

			// Getters
			/**
			 * Gets the requested size
			 * @return the requested size
			 */
			private int getSize() {
				return m_size;
			}

			/**
			 * Gets the requested address
			 * @return the requested address
			 */
			private long getAddress() {
				return m_address;
			}

			/**
			 * Checks if the request was executed
			 * @return true if the request was executed, false otherwise
			 */
			private boolean isExecuted() {
				boolean ret;

				m_lock.lock();

				ret = m_executed;

				m_lock.unlock();

				return ret;
			}

			// Methods
			/**
			 * Executes the request
			 * @param p_address
			 *            the requested address
			 */
			private void execute(final long p_address) {
				m_lock.lock();

				m_address = p_address;

				m_lock.unlock();
			}

		}

	}

}
