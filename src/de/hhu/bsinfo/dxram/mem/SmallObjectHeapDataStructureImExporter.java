
package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.soh.SmallObjectHeap;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Importer/Exporter wrapper to allow Importables/Exportables to be directly written
 * to the SmallObjectHeap.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
 */
public class SmallObjectHeapDataStructureImExporter implements Importer, Exporter {

	private SmallObjectHeap m_heap;
	private long m_allocatedMemoryStartAddress = -1;
	private int m_offset = -1;
	private int m_chunkSize = -1;

	/**
	 * Constructor
	 *
	 * @param p_heap                        The heap to access for the importer/exporter.
	 * @param p_allocatedMemoryStartAddress The start address of the allocated memory block to access.
	 * @param p_offset                      The start offset within the allocated block.
	 * @param p_chunkSize                   The total size of the chunk
	 */
	public SmallObjectHeapDataStructureImExporter(final SmallObjectHeap p_heap,
			final long p_allocatedMemoryStartAddress, final int p_offset, final int p_chunkSize) {
		m_heap = p_heap;
		m_allocatedMemoryStartAddress = p_allocatedMemoryStartAddress;
		m_offset = p_offset;
		m_chunkSize = p_chunkSize;
	}

	public void setAllocatedMemoryStartAddress(final long p_allocatedMemoryStartAddress) {
		m_allocatedMemoryStartAddress = p_allocatedMemoryStartAddress;
	}

	public void setOffset(final int p_offset) {
		m_offset = p_offset;
	}

	public void setChunkSize(final int p_chunkSize) {
		m_chunkSize = p_chunkSize;
	}

	@Override
	public int exportObject(final Exportable p_object) {
		return p_object.exportObject(this, m_chunkSize);
	}

	@Override
	public void writeByte(final byte p_v) {
		m_heap.writeByte(m_allocatedMemoryStartAddress, m_offset, p_v);
		m_offset += Byte.BYTES;
	}

	@Override
	public void writeShort(final short p_v) {
		m_heap.writeShort(m_allocatedMemoryStartAddress, m_offset, p_v);
		m_offset += Short.BYTES;
	}

	@Override
	public void writeInt(final int p_v) {
		m_heap.writeInt(m_allocatedMemoryStartAddress, m_offset, p_v);
		m_offset += Integer.BYTES;
	}

	@Override
	public void writeLong(final long p_v) {
		m_heap.writeLong(m_allocatedMemoryStartAddress, m_offset, p_v);
		m_offset += Long.BYTES;
	}

	@Override
	public void writeFloat(final float p_v) {
		throw new RuntimeException("Not supported.");
	}

	@Override
	public void writeDouble(final double p_v) {
		throw new RuntimeException("Not supported.");
	}

	@Override
	public int writeBytes(final byte[] p_array) {
		return writeBytes(p_array, 0, p_array.length);
	}

	@Override
	public int writeBytes(final byte[] p_array, final int p_offset, final int p_length) {
		int written = m_heap.writeBytes(m_allocatedMemoryStartAddress, m_offset, p_array, p_offset, p_length);
		if (written != -1) {
			m_offset += written * Byte.BYTES;
		}
		return written;
	}

	@Override
	public int importObject(final Importable p_object) {
		return p_object.importObject(this, m_chunkSize);
	}

	@Override
	public byte readByte() {
		byte v = m_heap.readByte(m_allocatedMemoryStartAddress, m_offset);
		m_offset += Byte.BYTES;
		return v;
	}

	@Override
	public short readShort() {
		short v = m_heap.readShort(m_allocatedMemoryStartAddress, m_offset);
		m_offset += Short.BYTES;
		return v;
	}

	@Override
	public int readInt() {
		int v = m_heap.readInt(m_allocatedMemoryStartAddress, m_offset);
		m_offset += Integer.BYTES;
		return v;
	}

	@Override
	public long readLong() {
		long v = m_heap.readLong(m_allocatedMemoryStartAddress, m_offset);
		m_offset += Long.BYTES;
		return v;
	}

	@Override
	public float readFloat() {
		throw new RuntimeException("Not supported.");
	}

	@Override
	public double readDouble() {
		throw new RuntimeException("Not supported.");
	}

	@Override
	public int readBytes(final byte[] p_array) {
		return readBytes(p_array, 0, p_array.length);
	}

	@Override
	public int readBytes(final byte[] p_array, final int p_offset, final int p_length) {
		int read = m_heap.readBytes(m_allocatedMemoryStartAddress, m_offset, p_array, p_offset, p_length);
		if (read != -1) {
			m_offset += read * Byte.BYTES;
		}
		return read;
	}

	@Override
	public int writeShorts(final short[] p_array) {
		return writeShorts(p_array, 0, p_array.length);
	}

	@Override
	public int writeInts(final int[] p_array) {
		return writeInts(p_array, 0, p_array.length);
	}

	@Override
	public int writeLongs(final long[] p_array) {
		return writeLongs(p_array, 0, p_array.length);
	}

	@Override
	public int writeShorts(final short[] p_array, final int p_offset, final int p_length) {
		int written = m_heap.writeShorts(m_allocatedMemoryStartAddress, m_offset, p_array, p_offset, p_length);
		if (written != -1) {
			m_offset += written * Short.BYTES;
		}
		return written;
	}

	@Override
	public int writeInts(final int[] p_array, final int p_offset, final int p_length) {
		int written = m_heap.writeInts(m_allocatedMemoryStartAddress, m_offset, p_array, p_offset, p_length);
		if (written != -1) {
			m_offset += written * Integer.BYTES;
		}
		return written;
	}

	@Override
	public int writeLongs(final long[] p_array, final int p_offset, final int p_length) {
		int written = m_heap.writeLongs(m_allocatedMemoryStartAddress, m_offset, p_array, p_offset, p_length);
		if (written != -1) {
			m_offset += written * Long.BYTES;
		}
		return written;
	}

	@Override
	public int readShorts(final short[] p_array) {
		return readShorts(p_array, 0, p_array.length);
	}

	@Override
	public int readInts(final int[] p_array) {
		return readInts(p_array, 0, p_array.length);
	}

	@Override
	public int readLongs(final long[] p_array) {
		return readLongs(p_array, 0, p_array.length);
	}

	@Override
	public int readShorts(final short[] p_array, final int p_offset, final int p_length) {
		int read = m_heap.readShorts(m_allocatedMemoryStartAddress, m_offset, p_array, p_offset, p_length);
		if (read != -1) {
			m_offset += read * Short.BYTES;
		}
		return read;
	}

	@Override
	public int readInts(final int[] p_array, final int p_offset, final int p_length) {
		int read = m_heap.readInts(m_allocatedMemoryStartAddress, m_offset, p_array, p_offset, p_length);
		if (read != -1) {
			m_offset += read * Integer.BYTES;
		}
		return read;
	}

	@Override
	public int readLongs(final long[] p_array, final int p_offset, final int p_length) {
		int read = m_heap.readLongs(m_allocatedMemoryStartAddress, m_offset, p_array, p_offset, p_length);
		if (read != -1) {
			m_offset += read * Long.BYTES;
		}
		return read;
	}
}
