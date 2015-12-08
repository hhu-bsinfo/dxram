package de.uniduesseldorf.dxgraph.test;

public class ByteArrayTest 
{
	public static void main(String[] args)
	{
		byte[] array = new byte[15];
		
		ByteArrayUtils.setInt(array, 1, 0xAABBCCDD);
		int v = ByteArrayUtils.getInt(array, 1);
		
		System.out.println(Integer.toHexString(v));
		
		ByteArrayUtils.setLong(array, 2, 0x1122334455667788L);
		long v2 = ByteArrayUtils.getLong(array, 2);
		System.out.println(Long.toHexString(v2));
	}
}
