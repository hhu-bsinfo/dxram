package de.uniduesseldorf.dxram.core.mem;

import de.uniduesseldorf.soh.SmallObjectHeap;
import de.uniduesseldorf.utils.serialization.Exportable;
import de.uniduesseldorf.utils.serialization.Exporter;
import de.uniduesseldorf.utils.serialization.Importable;
import de.uniduesseldorf.utils.serialization.Importer;

public class SmallObjectHeapDataStructureImExporter implements Importer, Exporter {

	private SmallObjectHeap m_heap = null;
	private long m_allocatedMemoryStartAddress = -1;
	private int m_offset = -1;
	private int m_chunkSize = -1;
	
	/**
	 * Constructor
	 * @param p_heap The heap to access for the importer/exporter.
	 * @param p_startAddress The start address of the allocated memory block to access.
	 * @param p_offset The start offset within the allocated block.
	 */
	public SmallObjectHeapDataStructureImExporter(final SmallObjectHeap p_heap, final long p_allocatedMemoryStartAddress, final int p_offset, final int p_chunkSize) {
		m_heap = p_heap;
		m_allocatedMemoryStartAddress = p_allocatedMemoryStartAddress;
		m_offset = p_offset;
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
			m_offset += written;
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
		if (read != -1)
			m_offset += read;
		return read;		
	}
}
