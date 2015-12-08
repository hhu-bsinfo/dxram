package de.uniduesseldorf.dxgraph.test;

import de.uniduesseldorf.utils.Endianness;

public class ByteArrayUtils 
{
	private ByteArrayUtils()
	{
		
	}
	
	public static void setInt(final byte[] p_array, final int p_offset, final int p_value)
	{
		if (Endianness.getEndianness() > 0) {
			p_array[p_offset] = (byte) ((p_value >> 24) & 0xFF);
			p_array[p_offset + 1] = (byte) ((p_value >> 16) & 0xFF);
			p_array[p_offset + 2] = (byte) ((p_value >> 8) & 0xFF);
			p_array[p_offset + 3] = (byte) (p_value & 0xFF);
		} else {
			p_array[p_offset] = (byte) (p_value & 0xFF);
			p_array[p_offset + 1] = (byte) ((p_value >> 8) & 0xFF);
			p_array[p_offset + 2] = (byte) ((p_value >> 16) & 0xFF);
			p_array[p_offset + 3] = (byte) ((p_value >> 24) & 0xFF);
		}
	}
	
	public static int getInt(final byte[] p_array, final int p_offset)
	{
		int v;
		
		v = 0;
		if (Endianness.getEndianness() > 0) {
			v |= ((p_array[p_offset] & 0xFF) << 24);
			v |= ((p_array[p_offset + 1] & 0xFF) << 16);
			v |= ((p_array[p_offset + 2] & 0xFF) << 8);
			v |= (p_array[p_offset + 3] & 0xFF);
		} else {
			v |= (p_array[p_offset] & 0xFF);
			v |= ((p_array[p_offset + 1] & 0xFF) << 8);
			v |= ((p_array[p_offset + 2] & 0xFF) << 16);
			v |= ((p_array[p_offset + 3] & 0xFF) << 24);
		}
		
		return v;
	}
	
	public static void setLong(final byte[] p_array, final int p_offset, final long p_value)
	{
		if (Endianness.getEndianness() > 0) {
			p_array[p_offset] = (byte) ((p_value >> 56) & 0xFF);
			p_array[p_offset + 1] = (byte) ((p_value >> 48) & 0xFF);
			p_array[p_offset + 2] = (byte) ((p_value >> 40) & 0xFF);
			p_array[p_offset + 3] = (byte) ((p_value >> 32) & 0xFF);
			p_array[p_offset + 4] = (byte) ((p_value >> 24) & 0xFF);
			p_array[p_offset + 5] = (byte) ((p_value >> 16) & 0xFF);
			p_array[p_offset + 6] = (byte) ((p_value >> 8) & 0xFF);
			p_array[p_offset + 7] = (byte) (p_value & 0xFF);
		} else {
			p_array[p_offset] = (byte) (p_value & 0xFF);
			p_array[p_offset + 1] = (byte) ((p_value >> 8) & 0xFF);
			p_array[p_offset + 2] = (byte) ((p_value >> 16) & 0xFF);
			p_array[p_offset + 3] = (byte) ((p_value >> 24) & 0xFF);
			p_array[p_offset + 4] = (byte) ((p_value >> 32) & 0xFF);
			p_array[p_offset + 5] = (byte) ((p_value >> 40) & 0xFF);
			p_array[p_offset + 6] = (byte) ((p_value >> 48) & 0xFF);
			p_array[p_offset + 7] = (byte) ((p_value >> 56) & 0xFF);
		}
	}
	
	public static long getLong(final byte[] p_array, final int p_offset)
	{
		long v;
		
		v = 0;
		if (Endianness.getEndianness() > 0) {
			v |= (((long) (p_array[p_offset] & 0xFF)) << 56);
			v |= (((long) (p_array[p_offset + 1] & 0xFF)) << 48);
			v |= (((long) (p_array[p_offset + 2] & 0xFF)) << 40);
			v |= (((long) (p_array[p_offset + 3] & 0xFF)) << 32);
			v |= (((long) (p_array[p_offset + 4] & 0xFF)) << 24);
			v |= (((long) (p_array[p_offset + 5] & 0xFF)) << 16);
			v |= (((long) (p_array[p_offset + 6] & 0xFF)) << 8);
			v |= ((long) (p_array[p_offset + 7] & 0xFF));
		} else {
			v |= ((long) (p_array[p_offset] & 0xFF));
			v |= (((long) (p_array[p_offset + 1] & 0xFF) << 8));
			v |= (((long) (p_array[p_offset + 2] & 0xFF) << 16));
			v |= (((long) (p_array[p_offset + 3] & 0xFF) << 24));
			v |= (((long) (p_array[p_offset + 4] & 0xFF) << 32));
			v |= (((long) (p_array[p_offset + 5] & 0xFF) << 40));
			v |= (((long) (p_array[p_offset + 6] & 0xFF) << 48));
			v |= (((long) (p_array[p_offset + 7] & 0xFF) << 56));
		}
		
		return v;
	}
}
