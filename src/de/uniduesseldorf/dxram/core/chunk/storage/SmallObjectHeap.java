
package de.uniduesseldorf.dxram.core.chunk.storage;

import java.io.File;

import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.utils.Contract;
import de.uniduesseldorf.dxram.utils.Tools;

/**
 * The raw memory is split into several segments to provide
 * non conflicting access for multiple threads. A arena manager
 * takes care of assigning the threads accessing to the segments
 * on allocation calls. Further synchronization for free, read and
 * write calls are handled in this class.
 *
 * @author Florian Klein 13.02.2014
 * @author @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.11.15
 */
public final class SmallObjectHeap {

	// Attributes
	// have a few attributes protected for the HeapWalker/Analyzer
	protected Storage m_memory;

	protected long m_segmentSize;
	protected SmallObjectHeapSegment[] m_segments;
	private ArenaManager m_arenaManager;

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
	 * @param p_size
	 *            the size of the memory
	 * @param p_segmentSize
	 * 			  The size for a single segment.
	 * @return the actual size of the memory
	 * @throws MemoryException
	 *             if the memory could not be initialized
	 */
	public long initialize(final long p_size, final long p_segmentSize) throws MemoryException {
		long ret;
		int segmentCount;
		long base;
		long remaining;
		long size;

		Contract.check(p_size >= 0, "invalid size given");
		Contract.check(p_segmentSize >= 0, "invalid segment size given");

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
		m_arenaManager = new ArenaManager(m_segments);

		MemoryStatistic.getInstance().initMemory(p_size);

		ret = m_memory.getSize();

		return ret;
	}

	/**
	 * Disengages the memory
	 * @throws MemoryException
	 *             if the memory could not be disengaged
	 */
	public void disengage() throws MemoryException {
		m_memory.free();
		m_memory = null;

		m_segments = null;
		m_arenaManager = null;
	}

	/**
	 * Dump a range of memory to a file.
	 * @param p_file
	 *            Destination file to dump to.
	 * @param p_addr
	 *            Start address.
	 * @param p_count
	 *            Number of bytes to dump.
	 * @throws MemoryException
	 *             If dumping memory failed.
	 */
	public void dump(final File p_file, final long p_addr, final long p_count) throws MemoryException {
		m_memory.dump(p_file, p_addr, p_count);
	}

	/**
	 * Allocate a memory block
	 * @param p_size
	 *            the size of the block
	 * @return the offset of the block
	 * @throws MemoryException
	 *             if the memory block could not be allocated
	 */
	public long malloc(final int p_size) throws MemoryException {
		long ret = -1;
		SmallObjectHeapSegment segment = null;
		long threadID;

		threadID = Thread.currentThread().getId();

		segment = m_arenaManager.enterArenaOnMalloc(threadID, p_size + SmallObjectHeapSegment.MAX_LENGTH_FIELD);
		try {
			// Try to allocate in the current segment
			ret = segment.malloc(p_size);

			// Try to allocate in another segment
			if (ret == -1) {
				segment = m_arenaManager.assignNewSegmentOnMalloc(threadID, segment, p_size + SmallObjectHeapSegment.MAX_LENGTH_FIELD);

				ret = segment.malloc(p_size);
				if (ret == -1) {
					printDebugInfos();
					throw new MemoryException("Could not allocate memory of size " + p_size);
				}
			}
		} finally {
			m_arenaManager.leaveArenaOnMalloc(threadID, segment);
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
	public void free(final long p_address) throws MemoryException {
		SmallObjectHeapSegment segment;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.lockManage();
		try {
			segment.free(p_address);
		} finally {
			segment.unlockManage();
		}
	}
	
	/**
	 * Get the size of an allocated block of memory.
	 * @param p_address Address of the block.
	 * @return Size of the block in bytes (payload only).
	 * @throws MemoryException If getting size failed.
	 */
	public int getSizeBlock(final long p_address) throws MemoryException {
		SmallObjectHeapSegment segment;
		int size = -1;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.lockAccess();
		try {
			size = segment.getSizeBlock(p_address);
		} finally {
			segment.unlockAccess();
		}	
		
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
	public void set(final long p_address, final long p_size, final byte p_value) throws MemoryException {
		SmallObjectHeapSegment segment;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.lockAccess();
		try {
			segment.set(p_address, p_size, p_value);
		} finally {
			segment.unlockAccess();
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
		SmallObjectHeapSegment segment;
		byte val;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.lockAccess();
		try {
			val = segment.readByte(p_address, p_offset);
		} finally {
			segment.unlockAccess();
		}

		return val;
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
		SmallObjectHeapSegment segment;
		short val;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.lockAccess();
		try {
			val = segment.readShort(p_address, p_offset);
		} finally {
			segment.unlockAccess();
		}

		return val;
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
		SmallObjectHeapSegment segment;
		int val;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.lockAccess();
		try {
			val = segment.readInt(p_address, p_offset);
		} finally {
			segment.unlockAccess();
		}

		return val;
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
		SmallObjectHeapSegment segment;
		long val;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.lockAccess();
		try {
			val = segment.readLong(p_address, p_offset);
		} finally {
			segment.unlockAccess();
		}

		return val;
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
		SmallObjectHeapSegment segment;
		byte[] vals;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.lockAccess();
		try {
			vals = segment.readBytes(p_address, p_offset);
		} finally {
			segment.unlockAccess();
		}

		return vals;
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
		SmallObjectHeapSegment segment;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.lockAccess();
		try {
			segment.writeByte(p_address, p_offset, p_value);
		} finally {
			segment.unlockAccess();
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
		SmallObjectHeapSegment segment;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.lockAccess();
		try {
			segment.writeShort(p_address, p_offset, p_value);
		} finally {
			segment.unlockAccess();
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
		SmallObjectHeapSegment segment;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.lockAccess();
		try {
			segment.writeInt(p_address, p_offset, p_value);
		} finally {
			segment.unlockAccess();
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
		SmallObjectHeapSegment segment;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.lockAccess();
		try {
			segment.writeLong(p_address, p_offset, p_value);
		} finally {
			segment.unlockAccess();
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
		SmallObjectHeapSegment segment;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.lockAccess();
		try {
			segment.writeBytes(p_address, p_offset, p_value, p_length);
		} finally {
			segment.unlockAccess();
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
		SmallObjectHeapSegment segment;
		int val;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.lockAccess();
		try {
			val = segment.getCustomState(p_address);
		} finally {
			segment.unlockAccess();
		}

		return val;
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
		SmallObjectHeapSegment segment;

		segment = m_segments[(int) (p_address / m_segmentSize)];
		segment.lockManage();
		try {
			segment.setCustomState(p_address, p_customState);
		} finally {
			segment.lockManage();
		}
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

	/**
	 * Locks the read lock
	 * @param p_address
	 *            the address of the lock
	 */
	public void readLock(final long p_address) {
		m_memory.readLock(p_address);
	}

	/**
	 * Unlocks the read lock
	 * @param p_address
	 *            the address of the lock
	 */
	public void readUnlock(final long p_address) {
		m_memory.readUnlock(p_address);
	}

	/**
	 * Locks the write lock
	 * @param p_address
	 *            the address of the lock
	 */
	public void writeLock(final long p_address) {
		m_memory.writeLock(p_address);
	}

	/**
	 * Unlocks the write lock
	 * @param p_address
	 *            the address of the lock
	 */
	public void writeUnlock(final long p_address) {
		m_memory.writeUnlock(p_address);
	}

	// --------------------------------------------------------------------------------------------------------

	/**
	 * Gets the segment for the given address
	 * @param p_address
	 *            the address
	 * @return the segment
	 */
	protected int getSegment(final long p_address) {
		return (int) (p_address / m_segmentSize);
	}

	/**
	 * Gets the current segment status
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
