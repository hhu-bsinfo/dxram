package de.uniduesseldorf.soh;

import java.io.File;

import de.uniduesseldorf.dxram.core.exceptions.MemoryException;

/**
 * Interface to describe a type of storage/memory to store
 * data to.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.11.2015
 */
public interface Storage {
	/**
	 * Allocate/Initialize the storage.
	 * Make sure to call this before calling any other methods.
	 *
	 * @param p_size Size of the storage in bytes.
	 */
	void allocate(final long p_size);

	/**
	 * Free/Cleanup the storage.
	 * Make sure to call this before object destruction.
	 *
	 * @throws MemoryException If free'ing failed.
	 */
	void free();

	/**
	 * Dump a range of the storage to a file.
	 * @param p_file
	 *            Destination file to dump to.
	 * @param p_ptr
	 *            Start address.
	 * @param p_length
	 *            Number of bytes to dump.
	 * @throws MemoryException
	 *             If dumping memory failed.
	 */
	void dump(final File p_file, final long p_ptr, final long p_length);

	/**
	 * Get the total allocated size of the storage.
	 * @return Size of the storage.
	 */
	long getSize();

	/**
	 * Set a range of memory to a specified value.
	 *
	 * @param p_ptr Pointer to the start location.
	 * @param p_size Number of bytes of the range.
	 * @param p_value Value to set for specified range.
	 * @throws MemoryException If setting value for specified range failed.
	 */
	void set(final long p_ptr, final long p_size, final byte p_value);

	/**
	 * Read data from the storage into a byte array.
	 *
	 * @param p_ptr Start position in storage.
	 * @param p_length Number of bytes to read from specified start.
	 * @return New byte array with read bytes.
	 * @throws MemoryException If reading fails.
	 */
	byte[] readBytes(final long p_ptr, final int p_length);

	/**
	 * Read data from the storage into a byte array.
	 *
	 * @param p_ptr Start position in storage.
	 * @param p_array Array to read the data into.
	 * @param p_arrayOffset Start offset in array to start writing the bytes to.
	 * @param p_length Number of bytes to read from specified start.
	 * @throws MemoryException If reading fails.
	 */
	int readBytes(final long p_ptr, final byte[] p_array, final int p_arrayOffset, final int p_length);

	/**
	 * Read a single byte value.
	 *
	 * @param p_ptr Position to read from.
	 * @return Byte read.
	 * @throws MemoryException If reading fails.
	 */
	byte readByte(final long p_ptr);

	/**
	 * Read a single short value.
	 *
	 * @param p_ptr Position to read from.
	 * @return Short read.
	 * @throws MemoryException If reading fails.
	 */
	short readShort(final long p_ptr);

	/**
	 * Read a single int value.
	 *
	 * @param p_ptr Position to read from.
	 * @return Int read.
	 * @throws MemoryException If reading fails.
	 */
	int readInt(final long p_ptr);

	/**
	 * Read a single long value.
	 *
	 * @param p_ptr Position to read from.
	 * @return Long read.
	 * @throws MemoryException If reading fails.
	 */
	long readLong(final long p_ptr);

	/**
	 * Write an array of bytes to the storage.
	 *
	 * @param p_ptr Start address to write to.
	 * @param p_array Array with data to write.
	 * @throws MemoryException If writing fails.
	 */
	int writeBytes(final long p_ptr, final byte[] p_array);

	/**
	 * Write an array of bytes to the storage.
	 *
	 * @param p_ptr Start address to write to.
	 * @param p_array Array with data to write.
	 * @param p_arrayOffset Offset in array to start reading the data from.
	 * @param p_length Number of bytes to write.
	 * @throws MemoryException If writing fails.
	 */
	int writeBytes(final long p_ptr, final byte[] p_array, final int p_arrayOffset, final int p_length);

	/**
	 * Write a single byte value to the storage.
	 *
	 * @param p_ptr Address to write to.
	 * @param p_value Value to write.
	 * @throws MemoryException If writing fails.
	 */
	void writeByte(final long p_ptr, final byte p_value);

	/**
	 * Write a single short value to the storage.
	 *
	 * @param p_ptr Address to write to.
	 * @param p_value Value to write.
	 * @throws MemoryException If writing fails.
	 */
	void writeShort(final long p_ptr, final short p_value);

	/**
	 * Write a single int value to the storage.
	 *
	 * @param p_ptr Address to write to.
	 * @param p_value Value to write.
	 * @throws MemoryException If writing fails.
	 */
	void writeInt(final long p_ptr, final int p_value);

	/**
	 * Write a single long value to the storage.
	 *
	 * @param p_ptr Address to write to.
	 * @param p_value Value to write.
	 * @throws MemoryException If writing fails.
	 */
	void writeLong(final long p_ptr, final long p_value);

	/**
	 * Read a value with specified number of bytes length from the storage.
	 *
	 * @param p_ptr Address to read from.
	 * @param p_count Number of bytes the value is stored to.
	 * @return Value read.
	 * @throws MemoryException If reading fails.
	 */
	long readVal(final long p_ptr, final int p_count);

	/**
	 * Write a value with specified number of bytes length to the storage.
	 *
	 * @param p_ptr Address to write to.
	 * @param p_val Value to write.
	 * @param p_count Number of bytes the value should occupy.
	 * @throws MemoryException If writing fails.
	 */
	void writeVal(final long p_ptr, final long p_val, final int p_count);
}
