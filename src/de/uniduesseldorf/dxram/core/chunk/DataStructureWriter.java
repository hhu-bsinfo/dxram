package de.uniduesseldorf.dxram.core.chunk;

import de.uniduesseldorf.dxram.core.exceptions.MemoryException;

public interface DataStructureWriter 
{
	public void putByte(final long p_startAddress, final int p_offset, final byte p_value) throws MemoryException;
	
	public void putShort(final long p_startAddress, final int p_offset, final short p_value) throws MemoryException;
	
	public void putInt(final long p_startAddress, final int p_offset, final int p_value) throws MemoryException;
	
	public void putLong(final long p_startAddress, final int p_offset, final long p_value) throws MemoryException;
	
	public int putBytes(final long p_startAddress, final int p_offset, final byte[] p_array, final int p_arrayOffset, int p_length) throws MemoryException;
}
