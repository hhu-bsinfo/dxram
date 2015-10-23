
package de.uniduesseldorf.dxram.core.chunk.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.utils.Endianness;

/**
 * Implementation of a storage based on a random access file.
 * Important note: This is quite slow and will take up
 * as much disk space as memory is requested on initialization.
 * Used for testing/memory debugging only.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de>
 *
 */
public class StorageRandomAccessFile implements Storage {
	private RandomAccessFile m_file;
	private long m_size;

	/**
	 * Constructor
	 *
	 * @param p_file File to use as storage.
	 * @throws FileNotFoundException If creating random access file failed.
	 */
	public StorageRandomAccessFile(final File p_file) throws FileNotFoundException {
		m_file = new RandomAccessFile(p_file, "rwd");
	}

	@Override
	public void allocate(final long p_size) throws MemoryException {
		final byte[] buf = new byte[8192];
		long size = p_size;

		try {
			m_file.setLength(0);
			m_file.seek(0);

			while (size > 0) {
				if (size >= buf.length) {
					m_file.write(buf, 0, buf.length);
					size -= buf.length;
				} else {
					m_file.write(buf, 0, (int) size);
					size = 0;
				}
			}

			m_size = m_file.length();
		} catch (final IOException e) {
			throw new MemoryException("Could not initialize memory", e);
		}
	}

	@Override
	public void free() throws MemoryException {
		try {
			m_file.close();
		} catch (final IOException e) {
			throw new MemoryException("Could not free memory", e);
		}
	}

	@Override
	public void dump(final File p_file, final long p_ptr, final long p_length) throws MemoryException {
		assert p_ptr >= 0;
		assert p_ptr < m_size;
		assert p_ptr + p_length <= m_size;

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
	public void set(final long p_ptr, final long p_size, final byte p_value) throws MemoryException {
		assert p_ptr >= 0;
		assert p_ptr < m_size;
		assert p_ptr + p_size <= m_size;

		final byte[] buf = new byte[8192];
		Arrays.fill(buf, p_value);
		long size = p_size;

		try {
			m_file.seek(p_ptr);

			while (size > 0) {
				if (size >= buf.length) {
					m_file.write(buf, 0, buf.length);
					size -= buf.length;
				} else {
					m_file.write(buf, 0, (int) size);
					size = 0;
				}
			}

			m_size = m_file.length();
		} catch (final IOException e) {
			throw new MemoryException("Could not initialize memory", e);
		}
	}

	@Override
	public byte[] readBytes(final long p_ptr, final int p_length) throws MemoryException {
		assert p_ptr >= 0;
		assert p_ptr < m_size;
		assert p_ptr + p_length <= m_size;

		final byte[] data = new byte[p_length];

		try {
			m_file.seek(p_ptr);
			m_file.read(data);
		} catch (final IOException e) {
			throw new MemoryException("reading bytes failed " + e);
		}

		return data;
	}

	@Override
	public void readBytes(final long p_ptr, final byte[] p_array, final int p_arrayOffset, final int p_length) throws MemoryException {
		assert p_ptr >= 0;
		assert p_ptr < m_size;
		assert p_ptr + p_length <= m_size;

		try {
			m_file.seek(p_ptr);
			m_file.read(p_array, p_arrayOffset, p_length);
		} catch (final IOException e) {
			throw new MemoryException("reading bytes failed " + e);
		}
	}

	@Override
	public byte readByte(final long p_ptr) throws MemoryException {
		assert p_ptr >= 0;
		assert p_ptr < m_size;

		byte value = 0;

		try {
			m_file.seek(p_ptr);
			value = (byte) m_file.read();
		} catch (final IOException e) {
			throw new MemoryException("reading failed " + e);
		}

		return value;
	}

	@Override
	public short readShort(final long p_ptr) throws MemoryException {
		assert p_ptr >= 0;
		assert p_ptr + 1 < m_size;

		short value = 0;

		try {
			m_file.seek(p_ptr);
			value = m_file.readShort();
		} catch (final IOException e) {
			throw new MemoryException("reading failed " + e);
		}

		return value;
	}

	@Override
	public int readInt(final long p_ptr) throws MemoryException {
		assert p_ptr >= 0;
		assert p_ptr + 3 < m_size;

		int value = 0;

		try {
			m_file.seek(p_ptr);
			value = m_file.readInt();
		} catch (final IOException e) {
			throw new MemoryException("reading failed " + e);
		}

		return value;
	}

	@Override
	public long readLong(final long p_ptr) throws MemoryException {
		assert p_ptr >= 0;
		assert p_ptr + 7 < m_size;

		long value = 0;

		try {
			m_file.seek(p_ptr);
			value = m_file.readLong();
		} catch (final IOException e) {
			throw new MemoryException("reading failed " + e);
		}

		return value;
	}

	@Override
	public void writeBytes(final long p_ptr, final byte[] p_array) throws MemoryException {
		assert p_ptr >= 0;
		assert p_ptr + p_array.length <= m_size;

		try {
			m_file.seek(p_ptr);
			m_file.write(p_array);
		} catch (final IOException e) {
			throw new MemoryException("writing failed " + e);
		}
	}

	@Override
	public void writeBytes(final long p_ptr, final byte[] p_array, final int p_arrayOffset, final int p_length) throws MemoryException {
		assert p_ptr >= 0;
		assert p_ptr + p_array.length <= m_size;

		try {
			m_file.seek(p_ptr);
			m_file.write(p_array, p_arrayOffset, p_length);
		} catch (final IOException e) {
			throw new MemoryException("writing failed " + e);
		}
	}

	@Override
	public void writeByte(final long p_ptr, final byte p_value) throws MemoryException {
		assert p_ptr >= 0;
		assert p_ptr < m_size;

		try {
			m_file.seek(p_ptr);
			m_file.writeByte(p_value);
		} catch (final IOException e) {
			throw new MemoryException("writing failed " + e);
		}
	}

	@Override
	public void writeShort(final long p_ptr, final short p_value) throws MemoryException {
		assert p_ptr > 0;
		assert p_ptr + 1 < m_size;

		try {
			m_file.seek(p_ptr);
			m_file.writeShort(p_value);
		} catch (final IOException e) {
			throw new MemoryException("writing failed " + e);
		}
	}

	@Override
	public void writeInt(final long p_ptr, final int p_value) throws MemoryException {
		assert p_ptr >= 0;
		assert p_ptr + 3 < m_size;

		try {
			m_file.seek(p_ptr);
			m_file.writeInt(p_value);
		} catch (final IOException e) {
			throw new MemoryException("writing failed " + e);
		}
	}

	@Override
	public void writeLong(final long p_ptr, final long p_value) throws MemoryException {
		assert p_ptr >= 0;
		assert p_ptr + 7 < m_size;

		try {
			m_file.seek(p_ptr);
			m_file.writeLong(p_value);
		} catch (final IOException e) {
			throw new MemoryException("writing failed " + e);
		}
	}

	@Override
	public long readVal(final long p_ptr, final int p_count) throws MemoryException {
		assert p_ptr >= 0;
		assert p_ptr + p_count <= m_size;

		long val = 0;

		try {
			m_file.seek(p_ptr);

			// take endianness into account!!!
			if (Endianness.getEndianness() > 0) {
				for (int i = 0; i < p_count; i++) {
					// input little endian byte order
					// work around not having unsigned data types and "wipe"
					// the sign by & 0xFF
					val |= ((long) (m_file.readByte() & 0xFF)) << (8 * i);
				}
			} else {
				for (int i = 0; i < p_count; i++) {
					// input little endian byte order
					// work around not having unsigned data types and "wipe"
					// the sign by & 0xFF
					val |= ((long) (m_file.readByte() & 0xFF)) << (8 * (7 - i));
				}
			}
		} catch (final IOException e) {
			throw new MemoryException("reading failed " + e);
		}

		return val;
	}

	@Override
	public void writeVal(final long p_ptr, final long p_val, final int p_count) throws MemoryException {
		assert p_ptr >= 0;
		assert p_ptr + p_count <= m_size;

		try {
			m_file.seek(p_ptr);

			// take endianness into account!!!
			if (Endianness.getEndianness() > 0) {
				for (int i = 0; i < p_count; i++) {
					// output little endian byte order
					m_file.writeByte((byte) ((p_val >> (8 * i)) & 0xFF));
				}
			} else {
				for (int i = 0; i < p_count; i++) {
					// output little endian byte order
					m_file.writeByte((byte) (p_val >> (8 * (7 - i)) & 0xFF));
				}
			}
		} catch (final IOException e) {
			throw new MemoryException("reading failed " + e);
		}
	}

	@Override
	public void readLock(final long p_address) {
		// TODO not needed?
	}

	@Override
	public void readUnlock(final long p_address) {
		// TODO not needed?
	}

	@Override
	public void writeLock(final long p_address) {
		// TODO not needed?
	}

	@Override
	public void writeUnlock(final long p_address) {
		// TODO not needed?
	}

}
