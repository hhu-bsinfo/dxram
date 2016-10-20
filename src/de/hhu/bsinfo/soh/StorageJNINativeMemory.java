
package de.hhu.bsinfo.soh;

import java.io.File;

import de.hhu.bsinfo.utils.JNINativeMemory;

/**
 * Storage implementation for JNINativeMemory
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.11.2015
 */
public class StorageJNINativeMemory implements Storage {

	private long m_memoryBase = -1;
	private long m_memorySize = -1;

	@Override
	public void allocate(final long p_size) {
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
	public void dump(final File p_file, final long p_ptr, final long p_length) {
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
	public void set(final long p_ptr, final long p_size, final byte p_value) {
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		assert p_ptr + p_size <= m_memorySize;

		JNINativeMemory.set(m_memoryBase + p_ptr, p_value, p_size);
	}

	@Override
	public int readBytes(final long p_ptr, final byte[] p_array, final int p_arrayOffset, final int p_length) {
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		assert p_ptr + p_length <= m_memorySize;

		JNINativeMemory.read(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
		return p_length;
	}

	@Override
	public byte readByte(final long p_ptr) {
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;

		return JNINativeMemory.readByte(m_memoryBase + p_ptr);
	}

	@Override
	public short readShort(final long p_ptr) {
		assert p_ptr >= 0;
		assert p_ptr + 1 < m_memorySize;

		return JNINativeMemory.readShort(m_memoryBase + p_ptr);
	}

	@Override
	public int readInt(final long p_ptr) {
		assert p_ptr >= 0;
		assert p_ptr + 3 < m_memorySize;

		return JNINativeMemory.readInt(m_memoryBase + p_ptr);
	}

	@Override
	public long readLong(final long p_ptr) {
		assert p_ptr >= 0;
		assert p_ptr + 7 < m_memorySize;

		return JNINativeMemory.readLong(m_memoryBase + p_ptr);
	}

	@Override
	public int writeBytes(final long p_ptr, final byte[] p_array, final int p_arrayOffset, final int p_length) {
		assert p_ptr >= 0;
		assert p_ptr + p_length <= m_memorySize;

		JNINativeMemory.write(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
		return p_length;
	}

	@Override
	public void writeByte(final long p_ptr, final byte p_value) {
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;

		JNINativeMemory.writeByte(m_memoryBase + p_ptr, p_value);
	}

	@Override
	public void writeShort(final long p_ptr, final short p_value) {
		assert p_ptr >= 0;
		assert p_ptr + 1 < m_memorySize;

		JNINativeMemory.writeShort(m_memoryBase + p_ptr, p_value);
	}

	@Override
	public void writeInt(final long p_ptr, final int p_value) {
		assert p_ptr >= 0;
		assert p_ptr + 3 < m_memorySize;

		JNINativeMemory.writeInt(m_memoryBase + p_ptr, p_value);
	}

	@Override
	public void writeLong(final long p_ptr, final long p_value) {
		assert p_ptr >= 0;
		assert p_ptr + 7 < m_memorySize;

		JNINativeMemory.writeLong(m_memoryBase + p_ptr, p_value);
	}

	@Override
	public long readVal(final long p_ptr, final int p_count) {
		assert p_ptr >= 0;
		assert p_ptr + p_count <= m_memorySize;

		return JNINativeMemory.readValue(m_memoryBase + p_ptr, p_count);
	}

	@Override
	public void writeVal(final long p_ptr, final long p_val, final int p_count) {
		assert p_ptr >= 0;
		assert p_ptr + p_count <= m_memorySize;

		JNINativeMemory.writeValue(m_memoryBase + p_ptr, p_val, p_count);
	}

}
