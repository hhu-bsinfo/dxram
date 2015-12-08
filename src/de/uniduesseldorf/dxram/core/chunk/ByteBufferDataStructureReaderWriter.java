package de.uniduesseldorf.dxram.core.chunk;

import java.nio.ByteBuffer;

public class ByteBufferDataStructureReaderWriter implements DataStructureReader, DataStructureWriter
{
	private ByteBuffer m_buffer;
	
	public ByteBufferDataStructureReaderWriter(final ByteBuffer p_byteBuffer)
	{
		m_buffer = p_byteBuffer;
		m_buffer.position(0);
	}

	@Override
	public void putByte(final long p_startAddress, int p_offset, byte p_value) {
		m_buffer.position(p_offset);
		m_buffer.put(p_value);
	}

	@Override
	public void putShort(final long p_startAddress, int p_offset, short p_value) {
		m_buffer.position(p_offset);
		m_buffer.putShort(p_value);
	}

	@Override
	public void putInt(final long p_startAddress, int p_offset, int p_value) {
		m_buffer.position(p_offset);
		m_buffer.putInt(p_value);
	}

	@Override
	public void putLong(final long p_startAddress, int p_offset, long p_value) {
		m_buffer.position(p_offset);
		m_buffer.putLong(p_value);
	}

	@Override
	public int putBytes(final long p_startAddress, int p_offset, byte[] p_array, int p_arrayOffset, int p_length) {
		m_buffer.position(p_offset);
		m_buffer.put(p_array, p_arrayOffset, p_length);
		
		return p_length;
	}

	@Override
	public byte getByte(final long p_startAddress, int p_offset) {
		m_buffer.position(p_offset);
		return m_buffer.get();
	}

	@Override
	public short getShort(final long p_startAddress, int p_offset) {
		m_buffer.position(p_offset);
		return m_buffer.getShort();
	}

	@Override
	public int getInt(final long p_startAddress, int p_offset) {
		m_buffer.position(p_offset);
		return m_buffer.getInt();
	}

	@Override
	public long getLong(final long p_startAddress, int p_offset) {
		m_buffer.position(p_offset);
		return m_buffer.getLong();
	}

	@Override
	public int getBytes(final long p_startAddress, int p_offset, byte[] p_array, int p_arrayOffset, int p_length) {
		m_buffer.position(p_offset);
		m_buffer.get(p_array, p_arrayOffset, p_length);
		return p_length;
	}
}
