package de.uniduesseldorf.dxram.test.nothaas;

public class BitShiftTest 
{
	public static void main(String[] args)
	{
		System.out.println("bla");
		
		int end = 0x11223344;
		if ((end & 0xFF) == 0x44)
		{
			System.out.println("Big endian");
		}
		else
		{
			System.out.println("Little endian");
		}
		
		long value = 0x10005A;
		
		for (int i = 0; i < 8; i++)
		{
			//byte tmp = (byte) (value >> (8 * (7 - i)) & 0xFF);
			byte tmp = (byte) (value >> (8 * i) & 0xFF);
			System.out.println((int) tmp);
		}
		
		{
			byte b = (byte) 0xFA;
			System.out.println(b);
			
			int in = (int) b;
			System.out.println(in);
			
			int in2 = b & 0xFF;
			System.out.println(in2);
		}
	}
}
