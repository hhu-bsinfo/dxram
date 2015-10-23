package de.uniduesseldorf.dxram.core.chunk.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;

import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.utils.Endianness;
import de.uniduesseldorf.dxram.utils.locks.JNILock;
import de.uniduesseldorf.dxram.utils.unsafe.UnsafeHandler;

import sun.misc.Unsafe;

public class StorageUnsafeMemory implements Storage
{
	private static final Unsafe UNSAFE = UnsafeHandler.getInstance().getUnsafe();
	
	private long m_memoryBase;
	private long m_memorySize;
	
	/** 
	 * Default constructor
	 */
	public StorageUnsafeMemory()
	{
		m_memoryBase = -1;
		m_memorySize = -1;
	}
	
	@Override
	public void allocate(final long p_size) throws MemoryException
	{
		assert p_size > 0;

		try {
			m_memoryBase = UNSAFE.allocateMemory(p_size);
		} catch (final Throwable e) {
			throw new MemoryException("Could not initialize memory", e);
		}
		
		m_memorySize = p_size;
	}
	
	@Override
	public void free() throws MemoryException
	{
		try {
			UNSAFE.freeMemory(m_memoryBase);
		} catch (final Throwable e) {
			throw new MemoryException("Could not free memory", e);
		}
		m_memorySize = 0;
	}
	
	@Override
	public String toString()
	{
		return "m_memoryBase=0x" + Long.toHexString(m_memoryBase) + ", m_memorySize: " + m_memorySize;
	}
	
	@Override
	public void dump(final File p_file, final long p_ptr, final long p_length) throws MemoryException
	{
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		assert p_ptr + p_length <= m_memorySize;
		
		RandomAccessFile outFile = null;
		try {
			outFile = new RandomAccessFile(p_file, "rw");

			long offset = 0;
			while (offset < p_length) {
				outFile.writeByte(UNSAFE.getByte(m_memoryBase + p_ptr + offset));
				offset++;
			}
		} catch (final IOException e) {
			throw new MemoryException(e.getMessage());
		} finally {
			try {
				if (outFile != null) {
					outFile.close();
				}
			} catch (final IOException e) {}
		}
	}
	
	@Override
	public long getSize()
	{
		return m_memorySize;
	}
	
	@Override
	public void set(final long p_ptr, final long p_size, final byte p_value) throws MemoryException
	{
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		assert p_ptr + p_size <= m_memorySize;
		
		UNSAFE.setMemory(m_memoryBase + p_ptr, p_size, p_value);
	}
	
	@Override
	public byte[] readBytes(final long p_ptr, final int p_length) throws MemoryException
	{
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		assert p_ptr + p_length <= m_memorySize;
		
		byte[] array = new byte[p_length];
		
		for (int i = 0; i < p_length; i++)
		{
			array[i] = UNSAFE.getByte(m_memoryBase + p_ptr + i);
		}
		
		return array;
	}
	
	@Override
	public void readBytes(final long p_ptr, byte[] p_array, int p_arrayOffset, int p_length) throws MemoryException
	{
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		assert p_ptr + p_length <= m_memorySize;
		
		for (int i = 0; i < p_length; i++)
		{
			p_array[p_arrayOffset + i] = UNSAFE.getByte(m_memoryBase + p_ptr + i);
		}
	}
	
	@Override
	public byte readByte(final long p_ptr) throws MemoryException
	{
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		
		return UNSAFE.getByte(m_memoryBase + p_ptr);
	}
	
	@Override
	public short readShort(final long p_ptr) throws MemoryException
	{
		assert p_ptr >= 0;
		assert p_ptr + 1 < m_memorySize;
		
		return UNSAFE.getShort(m_memoryBase + p_ptr);
	}
	
	@Override
	public int readInt(final long p_ptr) throws MemoryException
	{
		assert p_ptr >= 0;
		assert p_ptr + 3 < m_memorySize;
		
		return UNSAFE.getInt(m_memoryBase + p_ptr);
	}
	
	@Override
	public long readLong(final long p_ptr) throws MemoryException
	{
		assert p_ptr >= 0;
		assert p_ptr + 7 < m_memorySize;
		
		return UNSAFE.getLong(m_memoryBase + p_ptr);
	}
	
	@Override
	public void writeBytes(final long p_ptr, final byte[] p_array) throws MemoryException
	{
		assert p_ptr >= 0;
		assert p_ptr + p_array.length <= m_memorySize;
		
		for (int i = 0; i < p_array.length; i++)
		{
			UNSAFE.putByte(m_memoryBase + p_ptr + i, p_array[i]);
		}
	}
	
	@Override
	public void writeBytes(final long p_ptr, final byte[] p_array, final int p_arrayOffset, final int p_length) throws MemoryException
	{
		assert p_ptr >= 0;
		assert p_ptr + p_length <= m_memorySize;
		
		for (int i = 0; i < p_length; i++)
		{
			UNSAFE.putByte(m_memoryBase + p_ptr + i, p_array[p_arrayOffset + i]);
		}
	}
	
	@Override
	public void writeByte(final long p_ptr, final byte p_value) throws MemoryException
	{
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		
		UNSAFE.putByte(m_memoryBase + p_ptr, p_value);
	}
	
	@Override
	public void writeShort(final long p_ptr, final short p_value) throws MemoryException
	{
		assert p_ptr >= 0;
		assert p_ptr + 1 < m_memorySize;
		
		UNSAFE.putShort(m_memoryBase + p_ptr, p_value);
	}
	
	@Override
	public void writeInt(final long p_ptr, final int p_value) throws MemoryException
	{
		assert p_ptr >= 0;
		assert p_ptr + 3 < m_memorySize;
		
		UNSAFE.putInt(m_memoryBase + p_ptr, p_value);
	}
	
	@Override
	public void writeLong(final long p_ptr, final long p_value) throws MemoryException
	{
		assert p_ptr >= 0;
		assert p_ptr + 7 < m_memorySize;
		
		UNSAFE.putLong(m_memoryBase + p_ptr, p_value);
	}
	
	@Override
	public long readVal(final long p_ptr, final int p_count) throws MemoryException {
		assert p_ptr >= 0;
		assert p_ptr + p_count <= m_memorySize;
		
		long val = 0;
		
		// take endianness into account!!!
		if (Endianness.getEndianness() > 0) {
			for (int i = 0; i < p_count; i++)
			{
				// work around not having unsigned data types and "wipe"
				// the sign by & 0xFF
				val |= ((((long) (UNSAFE.getByte(m_memoryBase + p_ptr + i) & 0xFF)) << (8 * i)));
			}
		} else {
			for (int i = 0; i < p_count; i++)
			{
				// work around not having unsigned data types and "wipe"
				// the sign by & 0xFF
				val |= ((((long) (UNSAFE.getByte(m_memoryBase + p_ptr + i) & 0xFF)) << (8 * (7 - i))));
			}
		}
		
		return val;
	}

	@Override
	public void writeVal(final long p_ptr, final long p_val, final int p_count) throws MemoryException {
		assert p_ptr >= 0;
		assert p_ptr + p_count <= m_memorySize;
		
		// take endianness into account!!!
		if (Endianness.getEndianness() > 0) {
			for (int i = 0; i < p_count; i++)
			{
				UNSAFE.putByte(m_memoryBase + p_ptr + i, (byte) ((p_val >> (8 * i)) & 0xFF));
			}
		} else {
			for (int i = 0; i < p_count; i++)
			{
				UNSAFE.putByte(m_memoryBase + p_ptr + i, (byte) (p_val >> (8 * (7 - i)) & 0xFF));
			}
		}
	}
	
	@Override
	public void readLock(final long p_address) {
		JNILock.readLock(m_memoryBase + p_address);
	}

	@Override
	public void readUnlock(final long p_address) {
		JNILock.readUnlock(m_memoryBase + p_address);
	}

	@Override
	public void writeLock(final long p_address) {
		JNILock.writeLock(m_memoryBase + p_address);
	}

	@Override
	public void writeUnlock(final long p_address) {
		JNILock.writeUnlock(m_memoryBase + p_address);
	}
}
