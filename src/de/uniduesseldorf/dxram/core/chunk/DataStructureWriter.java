package de.uniduesseldorf.dxram.core.chunk;

public interface DataStructureWriter 
{
	public void putByte(final int p_offset, final byte p_value);
	
	public void putShort(final int p_offset, final short p_value);
	
	public void putInt(final int p_offset, final int p_value);
	
	public void putLong(final int p_offset, final long p_value);
	
	public int putBytes(final int p_offset, final byte[] p_array, final int p_arrayOffset, int p_length);
}
