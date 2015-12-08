package de.uniduesseldorf.utils;

public class Endianness 
{
	/**
	 * Indicates big endian byte order if > 0 and little endian for < 0 for primitive
	 * types long, int, short.
	 */
	private static final int ENDIANNESS = ((0x11223344 & 0xFF) == 0x44) ? 1 : -1;
	
	/**
	 * Constant to indicate big endian byte order.
	 */
	public static final int BIG_ENDIAN = 1;
	
	/**
	 * Constant to indicate little endian byte order.
	 */
	public static final int LITTLE_ENDIAN = -1;
	
	/**
	 * Get the endianness used on the running instance.
	 * @return Endianness > 0 for big endian byte order, < 0 for little endian.
	 */
	public static int getEndianness()
	{
		return ENDIANNESS;
	}
}
