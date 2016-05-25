
package de.hhu.bsinfo.soh;

import de.hhu.bsinfo.utils.Tools;

import java.io.File;

/**
 * The raw memory is split into several segments to provide
 * non conflicting access for multiple threads. A arena manager
 * takes care of assigning the threads accessing to the segments
 * on allocation calls. Further synchronization for free, read and
 * write calls are handled in this class.
 *
 * @author Florian Klein 13.02.2014
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.11.15
 */
public final class SmallObjectHeap {

	// have a few attributes package scoped for the HeapWalker/Analyzer
	Storage m_memory;

	long m_segmentSize;
	SmallObjectHeapSegment[] m_segments;

	// Constructors

	/**
	 * Creates an instance of RawMemory
	 *
	 * @param p_storageInstance Storage instance used as memory.
	 */
	public SmallObjectHeap(final Storage p_storageInstance) {
		m_memory = p_storageInstance;
	}

	// Methods

	/**
	 * Initializes the memory
	 *
	 * @param p_size        the size of the memory
	 * @param p_segmentSize The size for a single segment.
	 * @return the actual size of the memory
	 */
	public long initialize(final long p_size, final long p_segmentSize) {
		long ret;
		int segmentCount;
		long base;
		long remaining;
		long size;

		if (p_size < 0 || p_segmentSize < 0) {
			ret = -1;
		} else {
			m_segmentSize = p_segmentSize;

			segmentCount = (int) (p_size / p_segmentSize);
			if (p_size % p_segmentSize > 0) {
				segmentCount++;
			}

			m_memory.allocate(p_size);
			m_memory.set(0, m_memory.getSize(), (byte) 0);

			// Initialize segments
			base = 0;
			remaining = p_size;
			m_segments = new SmallObjectHeapSegment[segmentCount];
			for (int i = 0; i < segmentCount; i++) {
				size = Math.min(p_segmentSize, remaining);
				m_segments[i] = new SmallObjectHeapSegment(m_memory, i, base, size);

				base += p_segmentSize;
				remaining -= p_segmentSize;
			}
			// m_arenaManager = new ArenaManager(m_segments);

			ret = m_memory.getSize();
		}

		return ret;
	}

	/**
	 * Disengages the memory
	 */
	public void disengage() {
		m_memory.free();
		m_memory = null;

		m_segments = null;
		// m_arenaManager = null;
	}

	/**
	 * Dump a range of memory to a file.
	 *
	 * @param p_file  Destination file to dump to.
	 * @param p_addr  Start address.
	 * @param p_count Number of bytes to dump.
	 */
	public void dump(final File p_file, final long p_addr, final long p_count) {
		m_memory.dump(p_file, p_addr, p_count);
	}

	/**
	 * Allocate a memory block
	 *
	 * @param p_size the size of the block
	 * @return the offset of the block
	 */
	public long malloc(final int p_size) {
		long ret = -1;

		for (SmallObjectHeapSegment segment : m_segments) {
			ret = segment.malloc(p_size);
			if (ret != -1) {
				break;
			}
		}

		return ret;
	}

	/**
	 * Frees a memory block
	 *
	 * @param p_address the address of the block
	 */
	public void free(final long p_address) {
		SmallObjectHeapSegment segment;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.free(p_address);
	}

	/**
	 * Get the size of an allocated block of memory.
	 *
	 * @param p_address Address of the block.
	 * @return Size of the block in bytes (payload only).
	 */
	public int getSizeBlock(final long p_address) {
		SmallObjectHeapSegment segment;
		int size = -1;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		size = segment.getSizeBlock(p_address);

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
		SmallObjectHeapSegment segment;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.set(p_address, p_size, p_value);
	}

	/**
	 * Read a single byte from the specified address + offset.
	 *
	 * @param p_address Address.
	 * @param p_offset  Offset to add to the address.
	 * @return Byte read.
	 */
	public byte readByte(final long p_address, final long p_offset) {
		SmallObjectHeapSegment segment;
		byte val;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		val = segment.readByte(p_address, p_offset);

		return val;
	}

	/**
	 * Read a single short from the specified address + offset.
	 *
	 * @param p_address Address.
	 * @param p_offset  Offset to add to the address.
	 * @return Short read.
	 */
	public short readShort(final long p_address, final long p_offset) {
		SmallObjectHeapSegment segment;
		short val;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		val = segment.readShort(p_address, p_offset);

		return val;
	}

	/**
	 * Read a single int from the specified address + offset.
	 *
	 * @param p_address Address.
	 * @param p_offset  Offset to add to the address.
	 * @return Int read.
	 */
	public int readInt(final long p_address, final long p_offset) {
		SmallObjectHeapSegment segment;
		int val;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		val = segment.readInt(p_address, p_offset);

		return val;
	}

	/**
	 * Read a long from the specified address + offset.
	 *
	 * @param p_address Address.
	 * @param p_offset  Offset to add to the address.
	 * @return Long read.
	 */
	public long readLong(final long p_address, final long p_offset) {
		SmallObjectHeapSegment segment;
		long val;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		val = segment.readLong(p_address, p_offset);

		return val;
	}

	/**
	 * Read data from the heap into a byte array.
	 *
	 * @param p_address     Address of an allocated block of memory to read from.
	 * @param p_offset      Offset with the block to start reading at.
	 * @param p_buffer      Buffer to read into.
	 * @param p_offsetArray Offset within the buffer to start at.
	 * @param p_length      Number of elements to read.
	 * @return Number of elements read.
	 */
	public int readBytes(final long p_address, final long p_offset, final byte[] p_buffer, final int p_offsetArray,
			final int p_length) {
		SmallObjectHeapSegment segment;
		int bytesRead = -1;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		bytesRead = segment.readBytes(p_address, p_offset, p_buffer, p_offsetArray, p_length);

		return bytesRead;
	}

	/**
	 * Read data from the heap into a short array.
	 *
	 * @param p_address     Address of an allocated block of memory to read from.
	 * @param p_offset      Offset with the block to start reading at.
	 * @param p_buffer      Buffer to read into.
	 * @param p_offsetArray Offset within the buffer to start at.
	 * @param p_length      Number of elements to read.
	 * @return Number of elements read.
	 */
	public int readShorts(final long p_address, final long p_offset, final short[] p_buffer, final int p_offsetArray,
			final int p_length) {
		SmallObjectHeapSegment segment;
		int elementsRead = -1;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		elementsRead = segment.readShorts(p_address, p_offset, p_buffer, p_offsetArray, p_length);

		return elementsRead;
	}

	/**
	 * Read data from the heap into an int array.
	 *
	 * @param p_address     Address of an allocated block of memory to read from.
	 * @param p_offset      Offset with the block to start reading at.
	 * @param p_buffer      Buffer to read into.
	 * @param p_offsetArray Offset within the buffer to start at.
	 * @param p_length      Number of elements to read.
	 * @return Number of elements read.
	 */
	public int readInts(final long p_address, final long p_offset, final int[] p_buffer, final int p_offsetArray,
			final int p_length) {
		SmallObjectHeapSegment segment;
		int elementsRead = -1;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		elementsRead = segment.readInts(p_address, p_offset, p_buffer, p_offsetArray, p_length);

		return elementsRead;
	}

	/**
	 * Read data from the heap into a long array.
	 *
	 * @param p_address     Address of an allocated block of memory to read from.
	 * @param p_offset      Offset with the block to start reading at.
	 * @param p_buffer      Buffer to read into.
	 * @param p_offsetArray Offset within the buffer to start at.
	 * @param p_length      Number of elements to read.
	 * @return Number of elements read.
	 */
	public int readLongs(final long p_address, final long p_offset, final long[] p_buffer, final int p_offsetArray,
			final int p_length) {
		SmallObjectHeapSegment segment;
		int elementsRead = -1;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		elementsRead = segment.readLongs(p_address, p_offset, p_buffer, p_offsetArray, p_length);

		return elementsRead;
	}

	/**
	 * Write a single byte to the specified address + offset.
	 *
	 * @param p_address Address.
	 * @param p_offset  Offset to add to the address.
	 * @param p_value   Byte to write.
	 */
	public void writeByte(final long p_address, final long p_offset, final byte p_value) {
		SmallObjectHeapSegment segment;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.writeByte(p_address, p_offset, p_value);
	}

	/**
	 * Write a short to the spcified address + offset.
	 *
	 * @param p_address Address.
	 * @param p_offset  Offset to add to the address.
	 * @param p_value   Short to write.
	 */
	public void writeShort(final long p_address, final long p_offset, final short p_value) {
		SmallObjectHeapSegment segment;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.writeShort(p_address, p_offset, p_value);
	}

	/**
	 * Write a single int to the specified address + offset.
	 *
	 * @param p_address Address.
	 * @param p_offset  Offset to add to the address.
	 * @param p_value   int to write.
	 */
	public void writeInt(final long p_address, final long p_offset, final int p_value) {
		SmallObjectHeapSegment segment;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.writeInt(p_address, p_offset, p_value);
	}

	/**
	 * Write a long value to the specified address + offset.
	 *
	 * @param p_address Address.
	 * @param p_offset  Offset to add to the address.
	 * @param p_value   Long value to write.
	 */
	public void writeLong(final long p_address, final long p_offset, final long p_value) {
		SmallObjectHeapSegment segment;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.writeLong(p_address, p_offset, p_value);
	}

	/**
	 * Write data from a byte array to a block of memory on the heap.
	 *
	 * @param p_address     Address of an allocated block of memory.
	 * @param p_offset      Offset to start writing at within the block.
	 * @param p_value       Array with data to write.
	 * @param p_offsetArray Offset within the array to start reading at.
	 * @param p_length      Number of elements to write.
	 * @return Number of elements written.
	 */
	public int writeBytes(final long p_address, final long p_offset, final byte[] p_value, final int p_offsetArray,
			final int p_length) {
		SmallObjectHeapSegment segment;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		return segment.writeBytes(p_address, p_offset, p_value, p_offsetArray, p_length);
	}

	/**
	 * Write data from a short array to a block of memory on the heap.
	 *
	 * @param p_address     Address of an allocated block of memory.
	 * @param p_offset      Offset to start writing at within the block.
	 * @param p_value       Array with data to write.
	 * @param p_offsetArray Offset within the array to start reading at.
	 * @param p_length      Number of elements to write.
	 * @return Number of elements written.
	 */
	public int writeShorts(final long p_address, final long p_offset, final short[] p_value, final int p_offsetArray,
			final int p_length) {
		SmallObjectHeapSegment segment;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		return segment.writeShorts(p_address, p_offset, p_value, p_offsetArray, p_length);
	}

	/**
	 * Write data from an int array to a block of memory on the heap.
	 *
	 * @param p_address     Address of an allocated block of memory.
	 * @param p_offset      Offset to start writing at within the block.
	 * @param p_value       Array with data to write.
	 * @param p_offsetArray Offset within the array to start reading at.
	 * @param p_length      Number of elements to write.
	 * @return Number of elements written.
	 */
	public int writeInts(final long p_address, final long p_offset, final int[] p_value, final int p_offsetArray,
			final int p_length) {
		SmallObjectHeapSegment segment;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		return segment.writeInts(p_address, p_offset, p_value, p_offsetArray, p_length);
	}

	/**
	 * Write data from a long array to a block of memory on the heap.
	 *
	 * @param p_address     Address of an allocated block of memory.
	 * @param p_offset      Offset to start writing at within the block.
	 * @param p_value       Array with data to write.
	 * @param p_offsetArray Offset within the array to start reading at.
	 * @param p_length      Number of elements to write.
	 * @return Number of elements written.
	 */
	public int writeLongs(final long p_address, final long p_offset, final long[] p_value, final int p_offsetArray,
			final int p_length) {
		SmallObjectHeapSegment segment;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		return segment.writeLongs(p_address, p_offset, p_value, p_offsetArray, p_length);
	}

	/**
	 * Get the user definable state of a specified address referring
	 * a malloc'd block of memory.
	 *
	 * @param p_address Address of malloc'd block of memory.
	 * @return User definable state stored for that block (valid values: 0, 1, 2. invalid: -1)
	 */
	public int getCustomState(final long p_address) {
		SmallObjectHeapSegment segment;
		int val;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		val = segment.getCustomState(p_address);

		return val;
	}

	/**
	 * Set the user definable state for a specified address referring
	 * a malloc'd block of memory.
	 *
	 * @param p_address     Address of malloc'd block of memory.
	 * @param p_customState State to set for that block of memory (valid values: 0, 1, 2.
	 *                      all other values invalid).
	 */
	public void setCustomState(final long p_address, final int p_customState) {
		SmallObjectHeapSegment segment;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.setCustomState(p_address, p_customState);
	}

	/**
	 * Get the total space available in bytes.
	 *
	 * @return Total space in bytes.
	 */
	public long getTotalMemory() {
		return m_segmentSize * m_segments.length;
	}

	/**
	 * Get the amount of free memory in bytes.
	 *
	 * @return Free memory in bytes.
	 */
	public long getFreeMemory() {
		long size = 0;

		for (SmallObjectHeapSegment segment : m_segments) {
			size += segment.getStatus().getFreeSpace();
		}

		return size;
	}

	/**
	 * Get the total amount of memory usable for actual data.
	 *
	 * @return Total amount of memory usable for data in bytes.
	 */
	public long getTotalPayloadMemory() {
		long size = 0;

		for (SmallObjectHeapSegment segment : m_segments) {
			size += segment.getSizeAllocatedPayload();
		}

		return size;
	}

	/**
	 * Get the total number of active/allocated memory blocks.
	 *
	 * @return Number of allocated memory blocks.
	 */
	public long getNumberOfActiveMemoryBlocks() {
		long size = 0;

		for (SmallObjectHeapSegment segment : m_segments) {
			size += segment.getNumActiveMemoryBlocks();
		}

		return size;
	}

	@Override
	public String toString() {
		StringBuilder output;
		SmallObjectHeapSegment.Status[] stati;
		long freeSpace;
		long freeBlocks;

		stati = getSegmentStatus();
		freeSpace = 0;
		freeBlocks = 0;
		for (SmallObjectHeapSegment.Status status : stati) {
			freeSpace += status.getFreeSpace();
			freeBlocks += status.getFreeBlocks();
		}

		output = new StringBuilder();
		output.append("RawMemory (" + m_memory + ")");
		output.append("\nSegment Count: " + m_segments.length + " each of size " + Tools.readableSize(m_segmentSize));
		output.append("\nFree Space: " + Tools.readableSize(freeSpace) + " in " + freeBlocks + " blocks");

		for (int i = 0; i < stati.length; i++) {
			output.append("\n\t" + m_segments[i]);
		}

		return output.toString();
	}

	/**
	 * Prints debug infos
	 */
	public void printDebugInfos() {
		System.out.println("\n" + this);
	}

	/**
	 * Gets the current fragmentation of all segments
	 *
	 * @return the fragmentation
	 */
	public double[] getFragmentation() {
		double[] ret;

		ret = new double[m_segments.length];
		for (int i = 0; i < m_segments.length; i++) {
			ret[i] = m_segments[i].getFragmentation();
		}

		return ret;
	}

	// --------------------------------------------------------------------------------------------------------

	/**
	 * Gets the segment for the given address
	 *
	 * @param p_address the address
	 * @return the segment
	 */
	protected int getSegment(final long p_address) {
		return (int) (p_address / m_segmentSize);
	}

	/**
	 * Gets the current segment status
	 *
	 * @return the segment status
	 */
	protected SmallObjectHeapSegment.Status[] getSegmentStatus() {
		SmallObjectHeapSegment.Status[] ret;

		ret = new SmallObjectHeapSegment.Status[m_segments.length];
		for (int i = 0; i < m_segments.length; i++) {
			ret[i] = m_segments[i].getStatus();
		}

		return ret;
	}

}
