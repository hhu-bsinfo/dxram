package de.uniduesseldorf.dxram.core.chunk.storage;

import de.uniduesseldorf.dxram.core.chunk.DataStructureReader;
import de.uniduesseldorf.dxram.core.chunk.DataStructureWriter;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;

import de.uniduesseldorf.soh.SmallObjectHeap;

public class SmallObjectHeapDSReaderWriter implements DataStructureReader, DataStructureWriter
{
	private SmallObjectHeap m_heap;
	
	public SmallObjectHeapDSReaderWriter(final SmallObjectHeap p_heap)
	{
		m_heap = p_heap;
	}

	@Override
	public byte getByte(long p_startAddress, int p_offset) {
		return m_heap.readByte(p_startAddress, p_offset);
	}

	@Override
	public short getShort(long p_startAddress, int p_offset) {
		return m_heap.readShort(p_startAddress, p_offset);
	}

	@Override
	public int getInt(long p_startAddress, int p_offset) {
		return m_heap.readInt(p_startAddress, p_offset);
	}

	@Override
	public long getLong(long p_startAddress, int p_offset) {
		return m_heap.readLong(p_startAddress, p_offset);
	}

	@Override
	public int getBytes(long p_startAddress, int p_offset, byte[] p_array, int p_arrayOffset, int p_length) {
		return m_heap.readBytes(p_startAddress, p_offset, p_array, p_arrayOffset, p_length);
	}

	@Override
	public void putByte(long p_startAddress, int p_offset, byte p_value) {
		m_heap.writeByte(p_startAddress, p_offset, p_value);
	}

	@Override
	public void putShort(long p_startAddress, int p_offset, short p_value) {
		m_heap.writeShort(p_startAddress, p_offset, p_value);
	}

	@Override
	public void putInt(long p_startAddress, int p_offset, int p_value) {
		m_heap.writeInt(p_startAddress, p_offset, p_value);
	}

	@Override
	public void putLong(long p_startAddress, int p_offset, long p_value) {
		m_heap.writeLong(p_startAddress, p_offset, p_value);
	}

	@Override
	public int putBytes(long p_startAddress, int p_offset, byte[] p_array, int p_arrayOffset, int p_length) {
		return m_heap.writeBytes(p_startAddress, p_offset, p_array, p_arrayOffset, p_length);
	}
	

}
