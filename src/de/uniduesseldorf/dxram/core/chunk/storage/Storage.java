package de.uniduesseldorf.dxram.core.chunk.storage;

import java.io.File;

import de.uniduesseldorf.dxram.core.exceptions.MemoryException;

/** 
 * Interface to describe a type of storage/memory to store 
 * data to.
 * 
 * @author Stefan Nothaas <stefan.nothaas@hhu.de>
 */
public interface Storage 
{
	/** 
	 * Allocate/Initialize the storage.
	 * Make sure to call this before calling any other methods.
	 * 
	 * @param p_size Size of the storage in bytes.
	 * @throws MemoryException If allocation failed.
	 */
	public void allocate(final long p_size) throws MemoryException;
	
	/** 
	 * Free/Cleanup the storage.
	 * Make sure to call this before object destruction. 
	 * 
	 * @throws MemoryException If free'ing failed.
	 */
	public void free() throws MemoryException;
	
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
	public void dump(final File p_file, final long p_ptr, final long p_length) throws MemoryException;
	
	/** 
	 * Get the total allocated size of the storage.
	 * @return Size of the storage.
	 */
	public long getSize();
	
	/**
	 * Set a range of memory to a specified value.
	 * 
	 * @param p_ptr Pointer to the start location.
	 * @param p_size Number of bytes of the range.
	 * @param p_value Value to set for specified range.
	 * @throws MemoryException If setting value for specified range failed.
	 */
	public void set(final long p_ptr, final long p_size, final byte p_value) throws MemoryException;
	
	/**
	 * Read data from the storage into a byte array.
	 * 
	 * @param p_ptr Start position in storage.
	 * @param p_length Number of bytes to read from specified start.
	 * @return New byte array with read bytes. 
	 * @throws MemoryException If reading fails.
	 */
	public byte[] readBytes(final long p_ptr, final int p_length) throws MemoryException;
	
	/**
	 * Read data from the storage into a byte array.
	 * 
	 * @param p_ptr Start position in storage.
	 * @param p_array Array to read the data into.
	 * @param p_arrayOffset Start offset in array to start writing the bytes to. 
	 * @param p_length Number of bytes to read from specified start.
	 * @throws MemoryException If reading fails.
	 */
	public void readBytes(final long p_ptr, final byte[] p_array, final int p_arrayOffset, final int p_length) throws MemoryException;
	
	/**
	 * Read a single byte value.
	 * 
	 * @param p_ptr Position to read from.
	 * @return Byte read.
	 * @throws MemoryException If reading fails.
	 */
	public byte readByte(final long p_ptr) throws MemoryException;
	
	/** 
	 * Read a single short value.
	 * 
	 * @param p_ptr Position to read from.
	 * @return Short read.
	 * @throws MemoryException If reading fails.
	 */
	public short readShort(final long p_ptr) throws MemoryException;
	
	/** 
	 * Read a single int value.
	 * 
	 * @param p_ptr Position to read from.
	 * @return Int read.
	 * @throws MemoryException If reading fails.
	 */
	public int readInt(final long p_ptr) throws MemoryException;
	
	/** 
	 * Read a single long value.
	 * 
	 * @param p_ptr Position to read from.
	 * @return Long read.
	 * @throws MemoryException If reading fails.
	 */
	public long readLong(final long p_ptr) throws MemoryException;
	
	/** 
	 * Write an array of bytes to the storage.
	 * 
	 * @param p_ptr Start address to write to.
	 * @param p_array Array with data to write.
	 * @throws MemoryException If writing fails.
	 */
	public void writeBytes(final long p_ptr, final byte[] p_array) throws MemoryException;
	
	/** 
	 * Write an array of bytes to the storage.
	 * 
	 * @param p_ptr Start address to write to.
	 * @param p_array Array with data to write.
	 * @param p_arrayOffset Offset in array to start reading the data from.
	 * @param p_length Number of bytes to write.
	 * @throws MemoryException If writing fails.
	 */
	public void writeBytes(final long p_ptr, final byte[] p_array, final int p_arrayOffset, final int p_length) throws MemoryException;
	
	/**
	 * Write a single byte value to the storage.
	 * 
	 * @param p_ptr Address to write to.
	 * @param p_value Value to write.
	 * @throws MemoryException If writing fails.
	 */
	public void writeByte(final long p_ptr, final byte p_value) throws MemoryException;
	
	/**
	 * Write a single short value to the storage.
	 * 
	 * @param p_ptr Address to write to.
	 * @param p_value Value to write.
	 * @throws MemoryException If writing fails.
	 */
	public void writeShort(final long p_ptr, final short p_value) throws MemoryException;
	
	/**
	 * Write a single int value to the storage.
	 * 
	 * @param p_ptr Address to write to.
	 * @param p_value Value to write.
	 * @throws MemoryException If writing fails.
	 */
	public void writeInt(final long p_ptr, final int p_value) throws MemoryException;
	
	/**
	 * Write a single long value to the storage.
	 * 
	 * @param p_ptr Address to write to.
	 * @param p_value Value to write.
	 * @throws MemoryException If writing fails.
	 */
	public void writeLong(final long p_ptr, final long p_value) throws MemoryException;
	
	/**
	 * Locks the read lock
	 * @param p_address
	 *            the address of the lock
	 */
	public void readLock(final long p_address);

	/**
	 * Unlocks the read lock
	 * @param p_address
	 *            the address of the lock
	 */
	public void readUnlock(final long p_address);

	/**
	 * Locks the write lock
	 * @param p_address
	 *            the address of the lock
	 */
	public void writeLock(final long p_address);

	/**
	 * Unlocks the write lock
	 * @param p_address
	 *            the address of the lock
	 */
	public void writeUnlock(final long p_address);
}
