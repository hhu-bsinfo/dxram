package de.hhu.bsinfo.dxram.lookup.overlay;

import de.hhu.bsinfo.dxram.data.Chunk;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class SuperpeerStorage {

	private int m_maxNumEntries;
	private int m_maxSizeBytes;

	private byte[][] m_storage;
	private int m_lastSlotPos;
	private int m_allocatedSizeBytes;

	public SuperpeerStorage(final int p_maxNumEntries, final int p_maxSizeBytes) {
		m_storage = new byte[p_maxNumEntries][];
	}

	public int create(final int p_size) {
		if (m_allocatedSizeBytes + p_size > m_maxSizeBytes) {
			// exceeding quota
			return -1;
		}

		// find next free slot
		for (int i = 0; i < m_maxNumEntries; i++) {
			int idx = (i + m_lastSlotPos) % m_maxNumEntries;
			if (m_storage[idx] == null) {
				m_storage[idx] = new byte[p_size];
				m_lastSlotPos = idx;
				return idx;
			}
		}

		// no free slot
		return -1;
	}

	public int put(final int p_id, final byte[] p_buffer) {
		if (p_id < 0 || p_id > m_maxNumEntries) {
			return -1;
		}

		int written = -1;
		if (m_storage[p_id] != null) {
			if (p_buffer.length > m_storage[p_id].length) {
				written = m_storage[p_id].length;
			} else {
				written = p_buffer.length;
			}

			System.arraycopy(p_buffer, 0, m_storage[p_id], 0, written);
		}

		return written;
	}

	public int get(final int p_id, final byte[] p_buffer) {
		if (p_id < 0 || p_id > m_maxNumEntries) {
			return -1;
		}

		int read = -1;

		if (m_storage[p_id] != null) {
			if (p_buffer.length > m_storage[p_id].length) {
				read = m_storage[p_id].length;
			} else {
				read = p_buffer.length;
			}

			System.arraycopy(m_storage[p_id], 0, p_buffer, 0, read);
		}

		return read;
	}

	public Chunk get(final int p_id) {
		if (p_id < 0 || p_id > m_maxNumEntries) {
			return null;
		}

		Chunk data = null;

		if (m_storage[p_id] != null) {
			byte[] array = Arrays.copyOf(m_storage[p_id], m_storage[p_id].length);
			data = new Chunk(ByteBuffer.wrap(array));
		}

		return data;
	}

	public int size(final int p_id) {
		if (p_id < 0 || p_id > m_maxNumEntries) {
			return -1;
		}

		int size = -1;

		if (m_storage[p_id] != null) {
			size = m_storage[p_id].length;
		}

		return size;
	}

	public boolean remove(final int p_id) {
		if (p_id < 0 || p_id > m_maxNumEntries) {
			return false;
		}

		m_storage[p_id] = null;
		return true;
	}
}
