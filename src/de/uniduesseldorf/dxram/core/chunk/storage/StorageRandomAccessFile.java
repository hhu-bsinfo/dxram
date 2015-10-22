package de.uniduesseldorf.dxram.core.chunk.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import de.uniduesseldorf.dxram.core.exceptions.MemoryException;

public class StorageRandomAccessFile implements Storage
{
	private RandomAccessFile m_file;
	private long m_size;
	
	public StorageRandomAccessFile(final File p_file) throws FileNotFoundException
	{
		m_file = new RandomAccessFile(p_file, "rw");
	}
	
	@Override
	public void allocate(long p_size) throws MemoryException {
		byte[] buf = new byte[8192];
		long size = p_size;
		
		try
		{
			m_file.setLength(0);
			m_file.seek(0);
			
			while (size > 0)
			{
				if (size >= buf.length)
				{
					m_file.write(buf, 0, buf.length);
					size -= buf.length;
				}
				else
				{
					m_file.write(buf, 0, (int) size);
					size = 0;
				}
			}
			
			m_size = m_file.length();
		}
		catch (final IOException e)
		{
			throw new MemoryException("Could not initialize memory", e);
		}
	}

	@Override
	public void free() throws MemoryException {
		try {
			m_file.close();
		} catch (IOException e) {
			throw new MemoryException("Could not free memory", e);
		}
	}

	@Override
	public void dump(File p_file, long p_ptr, long p_length) throws MemoryException {
		assert p_ptr > 0;
		assert p_ptr < m_size;
		assert p_ptr + p_length < m_size;
		
		RandomAccessFile outFile = null;
		try {
			outFile = new RandomAccessFile(p_file, "rw");

			m_file.seek(p_ptr);
			int offset = 0;
			while (offset < p_length) {
				outFile.writeByte((byte) m_file.read());
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
	public long getSize() {
		return m_size;
	}

	@Override
	public void set(long p_ptr, long p_size, byte p_value) throws MemoryException {
		assert p_ptr > 0;
		assert p_ptr < m_size;
		assert p_ptr + p_size < m_size;
		
		byte[] buf = new byte[8192];
		Arrays.fill(buf, p_value);
		long size = p_size;
		
		try
		{
			m_file.seek(0);
			
			while (size > 0)
			{
				if (size >= buf.length)
				{
					m_file.write(buf, 0, buf.length);
					size -= buf.length;
				}
				else
				{
					m_file.write(buf, 0, (int) size);
					size = 0;
				}
			}
			
			m_size = m_file.length();
		}
		catch (final IOException e)
		{
			throw new MemoryException("Could not initialize memory", e);
		}
	}

	@Override
	public byte[] readBytes(long p_ptr, int p_length) throws MemoryException {
		assert p_ptr > 0;
		assert p_ptr < m_size;
		assert p_ptr + p_length < m_size;
		
		byte[] data = new byte[p_length];
		
		try {			
			m_file.seek(p_ptr);
			m_file.read(data);
		} catch (IOException e) {
			throw new MemoryException("reading bytes failed " + e);
		}
		
		return data;
	}

	@Override
	public void readBytes(long p_ptr, byte[] p_array, int p_arrayOffset, int p_length) throws MemoryException {
		assert p_ptr > 0;
		assert p_ptr < m_size;
		assert p_ptr + p_length < m_size;
		
		try {
			m_file.seek(p_ptr);
			m_file.read(p_array, p_arrayOffset, p_length);
		} catch (IOException e) {
			throw new MemoryException("reading bytes failed " + e);
		}
	}

	@Override
	public byte readByte(long p_ptr) throws MemoryException {
		assert p_ptr > 0;
		assert p_ptr < m_size;
		
		byte value = 0;
		
		try {
			m_file.seek(p_ptr);
			value = (byte) m_file.read();
		} catch (IOException e) {
			throw new MemoryException("reading failed " + e);
		}
		
		return value;
	}

	@Override
	public short readShort(long p_ptr) throws MemoryException {
		assert p_ptr > 0;
		assert p_ptr + 1 < m_size;
		
		short value = 0;
		
		try {
			m_file.seek(p_ptr);
			value = m_file.readShort();
		} catch (IOException e) {
			throw new MemoryException("reading failed " + e);
		}
		
		return value;
	}

	@Override
	public int readInt(long p_ptr) throws MemoryException {
		assert p_ptr > 0;
		assert p_ptr + 3 < m_size;
		
		int value = 0;
		
		try {
			m_file.seek(p_ptr);
			value = m_file.readInt();
		} catch (IOException e) {
			throw new MemoryException("reading failed " + e);
		}
		
		return value;
	}

	@Override
	public long readLong(long p_ptr) throws MemoryException {
		assert p_ptr > 0;
		assert p_ptr + 7 < m_size;
		
		long value = 0;
		
		try {
			m_file.seek(p_ptr);
			value = m_file.readLong();
		} catch (IOException e) {
			throw new MemoryException("reading failed " + e);
		}
		
		return value;
	}

	@Override
	public void writeBytes(long p_ptr, byte[] p_array) throws MemoryException {
		assert p_ptr > 0;
		assert p_ptr + p_array.length < m_size;
		
		try {
			m_file.seek(p_ptr);
			m_file.write(p_array);
		} catch (IOException e) {
			throw new MemoryException("writing failed " + e);
		}
	}

	@Override
	public void writeBytes(long p_ptr, byte[] p_array, int p_arrayOffset, int p_length) throws MemoryException {
		assert p_ptr > 0;
		assert p_ptr + p_array.length < m_size;
		
		try {
			m_file.seek(p_ptr);
			m_file.write(p_array, p_arrayOffset, p_length);
		} catch (IOException e) {
			throw new MemoryException("writing failed " + e);
		}	
	}

	@Override
	public void writeByte(long p_ptr, byte p_value) throws MemoryException {
		assert p_ptr > 0;
		assert p_ptr < m_size;
		
		try {
			m_file.seek(p_ptr);
			m_file.writeByte(p_value);
		} catch (IOException e) {
			throw new MemoryException("writing failed " + e);
		}
	}

	@Override
	public void writeShort(long p_ptr, short p_value) throws MemoryException {
		assert p_ptr > 0;
		assert p_ptr + 1 < m_size;
		
		try {
			m_file.seek(p_ptr);
			m_file.writeShort(p_value);
		} catch (IOException e) {
			throw new MemoryException("writing failed " + e);
		}
	}

	@Override
	public void writeInt(long p_ptr, int p_value) throws MemoryException {
		assert p_ptr > 0;
		assert p_ptr + 3 < m_size;
		
		try {
			m_file.seek(p_ptr);
			m_file.writeInt(p_value);
		} catch (IOException e) {
			throw new MemoryException("writing failed " + e);
		}
	}

	@Override
	public void writeLong(long p_ptr, long p_value) throws MemoryException {
		assert p_ptr > 0;
		assert p_ptr + 7 < m_size;
		
		try {
			m_file.seek(p_ptr);
			m_file.writeLong(p_value);
		} catch (IOException e) {
			throw new MemoryException("writing failed " + e);
		}
	}

	@Override
	public void readLock(long p_address) {
		// TODO not needed?
	}

	@Override
	public void readUnlock(long p_address) {
		// TODO not needed?
	}

	@Override
	public void writeLock(long p_address) {
		// TODO not needed?
	}

	@Override
	public void writeUnlock(long p_address) {
		// TODO not needed?
	}

}
