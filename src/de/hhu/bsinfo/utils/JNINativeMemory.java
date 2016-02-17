package de.hhu.bsinfo.utils;

public final class JNINativeMemory {

	/**
	 * Provide the path to the native implementation.
	 * @param p_pathNativeLibrary Path to the library with the native implementation.
	 */
	public static void load(final String p_pathNativeLibrary) {
		System.load(p_pathNativeLibrary);
	}
	
	private JNINativeMemory() {};
	
	public static native long alloc(final long p_size);
	
	public static native long free(final long p_addr);
	
	public static native void dump(final long p_addr, final long p_length, final String p_path);
	
	public static native void set(final long p_addr, final byte p_value, final long p_size);
	
	public static native void copy(final long p_addrDest, final long p_addrSrc, final long p_size);
	
	public static native void read(final long p_addr, byte[] p_array, final int p_arrayOffset, final int p_length);
	
	public static native void write(final long p_addr, byte[] p_array, final int p_arrayOffset, final int p_length);

	
	public static native byte read(final long p_addr);
	
	public static native short readShort(final long p_addr);
	
	public static native short readInt(final long p_addr);
	
	public static native short readLong(final long p_addr);
	
	
	public static native void write(final long p_addr, final byte p_value); 
	
	public static native void writeShort(final long p_addr, final short p_value);
	
	public static native void writeInt(final long p_addr, final int p_value);
	
	public static native void writeLong(final long p_addr, final long p_value);
}
