package de.uniduesseldorf.dxram.core.chunk;

import de.uniduesseldorf.dxram.core.exceptions.MemoryException;

public interface DataStructureReader 
{
	public byte getByte(final long p_startAddress, final int p_offset) throws MemoryException;
	
	public short getShort(final long p_startAddress, final int p_offset) throws MemoryException;
	
	public int getInt(final long p_startAddress, final int p_offset) throws MemoryException;
	
	public long getLong(final long p_startAddress, final int p_offset) throws MemoryException;
	
	public int getBytes(final long p_startAddress, final int p_offset, final byte[] p_array, final int p_arrayOffset, int p_length) throws MemoryException;
}
