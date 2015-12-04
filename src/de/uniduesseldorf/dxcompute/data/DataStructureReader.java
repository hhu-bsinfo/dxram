package de.uniduesseldorf.dxcompute.data;

public interface DataStructureReader 
{
	public byte getByte(final int p_offset);
	
	public short getShort(final int p_offset);
	
	public int getInt(final int p_offset);
	
	public long getLong(final int p_offset);
	
	public int getBytes(final int p_offset, final byte[] p_array, final int p_arrayOffset, int p_length);
}
