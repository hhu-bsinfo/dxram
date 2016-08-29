
package de.hhu.bsinfo.soh;

import java.io.File;

/**
 * Created by nothaas on 5/4/16.
 */
public class StorageJavaHeap implements Storage {
	private byte[] m_memory;
	private long m_memorySize;

	@Override
	public void allocate(final long p_size) {
		assert p_size > 0;

		if (p_size > Integer.MAX_VALUE) {
			throw new RuntimeException("Max storage size exceeded");
		}

		m_memorySize = p_size;
		m_memory = new byte[(int) p_size];
	}

	@Override
	public void free() {
		m_memory = null;
	}

	@Override
	public String toString() {
		return "m_memorySize: " + m_memorySize;
	}

	@Override
	public void dump(final File p_file, final long p_ptr, final long p_length) {
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		assert p_ptr + p_length <= m_memorySize;

		throw new RuntimeException("Not implemented");
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

		for (int i = 0; i < p_size; i++) {
			m_memory[((int) p_ptr) + i] = p_value;
		}
	}

	@Override
	public int readBytes(final long p_ptr, final byte[] p_array, final int p_arrayOffset, final int p_length) {
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;
		assert p_ptr + p_length <= m_memorySize;

		for (int i = 0; i < p_length; i++) {
			p_array[p_arrayOffset + i] = m_memory[((int) p_ptr) + i];
		}

		return p_length;
	}

	@Override
	public byte readByte(final long p_ptr) {
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;

		return m_memory[(int) p_ptr];
	}

	@Override
	public short readShort(final long p_ptr) {
		assert p_ptr >= 0;
		assert p_ptr + 1 < m_memorySize;

		short v = 0;
		v |= ((short) (m_memory[(int) p_ptr] & 0xFF)) << 8;
		v |= ((short) (m_memory[(int) p_ptr + 1] & 0xFF));

		return v;
	}

	@Override
	public int readInt(final long p_ptr) {
		assert p_ptr >= 0;
		assert p_ptr + 3 < m_memorySize;

		int v = 0;
		v |= ((int) (m_memory[(int) p_ptr] & 0xFF)) << 24;
		v |= ((int) (m_memory[(int) p_ptr + 1] & 0xFF)) << 16;
		v |= ((int) (m_memory[(int) p_ptr + 2] & 0xFF)) << 8;
		v |= ((int) (m_memory[(int) p_ptr + 3] & 0xFF));

		return v;
	}

	@Override
	public long readLong(final long p_ptr) {
		assert p_ptr >= 0;
		assert p_ptr + 7 < m_memorySize;

		long v = 0;
		v |= ((long) (m_memory[(int) p_ptr] & 0xFF)) << 56L;
		v |= ((long) (m_memory[(int) p_ptr + 1] & 0xFF)) << 48L;
		v |= ((long) (m_memory[(int) p_ptr + 2] & 0xFF)) << 40L;
		v |= ((long) (m_memory[(int) p_ptr + 3] & 0xFF)) << 32L;
		v |= ((long) (m_memory[(int) p_ptr + 4] & 0xFF)) << 24L;
		v |= ((long) (m_memory[(int) p_ptr + 5] & 0xFF)) << 16L;
		v |= ((long) (m_memory[(int) p_ptr + 6] & 0xFF)) << 8L;
		v |= ((long) (m_memory[(int) p_ptr + 7] & 0xFF));

		return v;
	}

	@Override
	public int writeBytes(final long p_ptr, final byte[] p_array, final int p_arrayOffset, final int p_length) {
		assert p_ptr >= 0;
		assert p_ptr + p_length <= m_memorySize;

		for (int i = 0; i < p_length; i++) {
			m_memory[((int) p_ptr) + i] = p_array[p_arrayOffset + i];
		}

		return p_length;
	}

	@Override
	public void writeByte(final long p_ptr, final byte p_value) {
		assert p_ptr >= 0;
		assert p_ptr < m_memorySize;

		m_memory[(int) p_ptr] = (byte) (p_value & 0xFF);
	}

	@Override
	public void writeShort(final long p_ptr, final short p_value) {
		assert p_ptr >= 0;
		assert p_ptr + 1 < m_memorySize;

		m_memory[(int) p_ptr] = (byte) ((p_value >> 8) & 0xFF);
		m_memory[(int) p_ptr + 1] = (byte) (p_value & 0xFF);
	}

	@Override
	public void writeInt(final long p_ptr, final int p_value) {
		assert p_ptr >= 0;
		assert p_ptr + 3 < m_memorySize;

		m_memory[(int) p_ptr] = (byte) ((p_value >> 24) & 0xFF);
		m_memory[(int) p_ptr + 1] = (byte) ((p_value >> 16) & 0xFF);
		m_memory[(int) p_ptr + 2] = (byte) ((p_value >> 8) & 0xFF);
		m_memory[(int) p_ptr + 3] = (byte) (p_value & 0xFF);
	}

	@Override
	public void writeLong(final long p_ptr, final long p_value) {
		assert p_ptr >= 0;
		assert p_ptr + 7 < m_memorySize;

		m_memory[(int) p_ptr] = (byte) ((p_value >> 56L) & 0xFF);
		m_memory[(int) p_ptr + 1] = (byte) ((p_value >> 48L) & 0xFF);
		m_memory[(int) p_ptr + 2] = (byte) ((p_value >> 40L) & 0xFF);
		m_memory[(int) p_ptr + 3] = (byte) ((p_value >> 32L) & 0xFF);
		m_memory[(int) p_ptr + 4] = (byte) ((p_value >> 24L) & 0xFF);
		m_memory[(int) p_ptr + 5] = (byte) ((p_value >> 16L) & 0xFF);
		m_memory[(int) p_ptr + 6] = (byte) ((p_value >> 8L) & 0xFF);
		m_memory[(int) p_ptr + 7] = (byte) (p_value & 0xFF);
	}

	@Override
	public long readVal(final long p_ptr, final int p_count) {
		assert p_ptr >= 0;
		assert p_ptr + p_count <= m_memorySize;

		long val = 0;
		for (int i = 0; i < p_count; i++) {
			// work around not having unsigned data types and "wipe"
			// the sign by & 0xFF
			val |= ((long) (m_memory[(int) p_ptr + i] & 0xFF)) << (8 * i);
		}

		return val;
	}

	@Override
	public void writeVal(final long p_ptr, final long p_val, final int p_count) {
		assert p_ptr >= 0;
		assert p_ptr + p_count <= m_memorySize;

		for (int i = 0; i < p_count; i++) {
			m_memory[(int) p_ptr + i] = (byte) ((p_val >> (8 * i)) & 0xFF);
		}
	}
}
