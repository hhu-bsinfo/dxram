package de.hhu.bsinfo.soh;

import java.io.File;

import de.hhu.bsinfo.utils.JNINativeMemory;

public class StorageJNINativeMemory implements Storage {

	private long m_memoryBase = -1;
	private long m_memorySize = -1;
	
	@Override
	public void allocate(long p_size) {
		assert p_size > 0;

		m_memoryBase = JNINativeMemory.alloc(p_size);
		if (m_memoryBase == 0) {
			throw new MemoryRuntimeException("Could not initialize memory");
		}

		m_memorySize = p_size;
	}

	@Override
	public void free() {
		JNINativeMemory.free(m_memoryBase);
		m_memoryBase = -1;
		m_memorySize = -1;
	}
	
	@Override
	public String toString() {
		return "m_memoryBase=0x" + Long.toHexString(m_memoryBase) + ", m_memorySize: " + m_memorySize;
	}

	@Override
	public void dump(File p_file, long p_ptr, long p_length) {
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		assert p_ptr + p_length <= m_memorySize;
		
		JNINativeMemory.dump(m_memoryBase + p_ptr, p_length, p_file.getAbsolutePath());
	}

	@Override
	public long getSize() {
		return m_memorySize;
	}

	@Override
	public void set(long p_ptr, long p_size, byte p_value) {
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		assert p_ptr + p_size <= m_memorySize;
		
		JNINativeMemory.set(m_memoryBase + p_ptr, p_value, p_size);
	}

	@Override
	public byte[] readBytes(long p_ptr, int p_length) {
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		assert p_ptr + p_length <= m_memorySize;
		
		final byte[] array = new byte[p_length];
		
		JNINativeMemory.read(m_memoryBase + p_ptr, array, 0, p_length);
		return array;
	}

	@Override
	public int readBytes(long p_ptr, byte[] p_array, int p_arrayOffset, int p_length) {
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		assert p_ptr + p_length <= m_memorySize;
		
		JNINativeMemory.read(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
		return p_length;
	}

	@Override
	public byte readByte(long p_ptr) {
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		
		return JNINativeMemory.readByte(m_memoryBase + p_ptr);
	}

	@Override
	public short readShort(long p_ptr) {
		assert p_ptr >= 0;
		assert p_ptr + 1 < m_memorySize;
		
		return JNINativeMemory.readShort(m_memoryBase + p_ptr);
	}

	@Override
	public int readInt(long p_ptr) {
		assert p_ptr >= 0;
		assert p_ptr + 3 < m_memorySize;
		
		return JNINativeMemory.readInt(m_memoryBase + p_ptr);
	}

	@Override
	public long readLong(long p_ptr) {
		assert p_ptr >= 0;
		assert p_ptr + 7 < m_memorySize;
		
		return JNINativeMemory.readLong(m_memoryBase + p_ptr);
	}

	@Override
	public int writeBytes(long p_ptr, byte[] p_array) {
		return writeBytes(p_ptr, p_array, 0, p_array.length);
	}

	@Override
	public int writeBytes(long p_ptr, byte[] p_array, int p_arrayOffset, int p_length) {
		assert p_ptr >= 0;
		assert p_ptr + p_length <= m_memorySize;
		
		JNINativeMemory.write(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
		return p_length;
	}

	@Override
	public void writeByte(long p_ptr, byte p_value) {
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		
		JNINativeMemory.writeByte(m_memoryBase + p_ptr, p_value);
	}

	@Override
	public void writeShort(long p_ptr, short p_value) {
		assert p_ptr >= 0;
		assert p_ptr + 1 < m_memorySize;
		
		JNINativeMemory.writeShort(m_memoryBase + p_ptr, p_value);
	}

	@Override
	public void writeInt(long p_ptr, int p_value) {
		assert p_ptr >= 0;
		assert p_ptr + 3 < m_memorySize;
		
		JNINativeMemory.writeInt(m_memoryBase + p_ptr, p_value);
	}

	@Override
	public void writeLong(long p_ptr, long p_value) {
		assert p_ptr >= 0;
		assert p_ptr + 7 < m_memorySize;
		
		JNINativeMemory.writeLong(m_memoryBase + p_ptr, p_value);
	}

	@Override
	public long readVal(long p_ptr, int p_count) {
		assert p_ptr >= 0;
		assert p_ptr + p_count <= m_memorySize;

		return JNINativeMemory.readValue(m_memoryBase + p_ptr, p_count);
	}

	@Override
	public void writeVal(long p_ptr, long p_val, int p_count) {
		assert p_ptr >= 0;
		assert p_ptr + p_count <= m_memorySize;

		JNINativeMemory.writeValue(m_memoryBase + p_ptr, p_val, p_count);
	}

	@Override
	public short[] readShorts(long p_ptr, int p_length) {
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		assert p_ptr + p_length * Short.BYTES <= m_memorySize;
		
		final short[] array = new short[p_length];
		
		JNINativeMemory.readShorts(m_memoryBase + p_ptr, array, 0, p_length);
		return array;
	}

	@Override
	public int[] readInts(long p_ptr, int p_length) {
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		assert p_ptr + p_length * Integer.BYTES <= m_memorySize;
		
		final int[] array = new int[p_length];
		
		JNINativeMemory.readInts(m_memoryBase + p_ptr, array, 0, p_length);
		return array;
	}

	@Override
	public long[] readLongs(long p_ptr, int p_length) {
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		assert p_ptr + p_length * Long.BYTES <= m_memorySize;
		
		final long[] array = new long[p_length];
		
		JNINativeMemory.readLongs(m_memoryBase + p_ptr, array, 0, p_length);
		return array;
	}

	@Override
	public int readShorts(long p_ptr, short[] p_array, int p_arrayOffset, int p_length) {
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		assert p_ptr + p_length * Short.BYTES <= m_memorySize;
		
		JNINativeMemory.readShorts(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
		return p_length;
	}

	@Override
	public int readInts(long p_ptr, int[] p_array, int p_arrayOffset, int p_length) {
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		assert p_ptr + p_length * Integer.BYTES <= m_memorySize;
		
		JNINativeMemory.readInts(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
		return p_length;
	}

	@Override
	public int readLongs(long p_ptr, long[] p_array, int p_arrayOffset, int p_length) {
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		assert p_ptr + p_length * Long.BYTES <= m_memorySize;
		
		JNINativeMemory.readLongs(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
		return p_length;
	}

	@Override
	public int writeShorts(long p_ptr, short[] p_array) {
		return writeShorts(p_ptr, p_array, 0, p_array.length);
	}

	@Override
	public int writeInts(long p_ptr, int[] p_array) {
		return writeInts(p_ptr, p_array, 0, p_array.length);
	}

	@Override
	public int writeLongs(long p_ptr, long[] p_array) {
		return writeLongs(p_ptr, p_array, 0, p_array.length);
	}

	@Override
	public int writeShorts(long p_ptr, short[] p_array, int p_arrayOffset, int p_length) {
		assert p_ptr >= 0;
		assert p_ptr + p_length * Short.BYTES <= m_memorySize;
		
		JNINativeMemory.writeShorts(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
		return p_length;
	}

	@Override
	public int writeInts(long p_ptr, int[] p_array, int p_arrayOffset, int p_length) {
		assert p_ptr >= 0;
		assert p_ptr + p_length * Integer.BYTES <= m_memorySize;
		
		JNINativeMemory.writeInts(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
		return p_length;
	}

	@Override
	public int writeLongs(long p_ptr, long[] p_array, int p_arrayOffset, int p_length) {
		assert p_ptr >= 0;
		assert p_ptr + p_length * Long.BYTES <= m_memorySize;
		
		JNINativeMemory.writeLongs(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
		return p_length;
	}

}
