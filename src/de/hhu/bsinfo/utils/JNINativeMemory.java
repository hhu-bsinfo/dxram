package de.hhu.bsinfo.utils;

/**
 * Replacement for Java's undocumented Unsafe class
 * (this supports proper memcpy'ing of byte arrays). 
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 *
 */
public final class JNINativeMemory {

	/**
	 * Provide the path to the native implementation.
	 * @param p_pathNativeLibrary Path to the library with the native implementation.
	 */
	public static void load(final String p_pathNativeLibrary) {
		System.load(p_pathNativeLibrary);
	}
	
	/**
	 * Static class, private constuctor.
	 */
	private JNINativeMemory() {};
	
	/**
	 * Allocate memory on the native operating system's heap.
	 * @param p_size Number of bytes to allocate.
	 * @return Pointer/Handle of the memory location with the allocated space.
	 */
	public static native long alloc(final long p_size);
	
	/**
	 * Free a previously allocated memory block.
	 * @param p_addr Address of the memory block.
	 */
	public static native void free(final long p_addr);
	
	/**
	 * Dump the contents of the native memory to a file (mainly used for debugging).
	 * @param p_addr Startaddress within our user space memory.
	 * @param p_length Number of bytes to dump.
	 * @param p_path Path to the file to dump to (will be overwritten/created).
	 */
	public static native void dump(final long p_addr, final long p_length, final String p_path);
	
	/**
	 * Memset equivalent.
	 * @param p_addr Address.
	 * @param p_value Value to set.
	 * @param p_size Number of bytes to set to the value specified.
	 */
	public static native void set(final long p_addr, final byte p_value, final long p_size);
	
	/**
	 * Memcpy equivalent.
	 * @param p_addrDest Destination address to copy to.
	 * @param p_addrSrc Source address to read from.
	 * @param p_size Number of bytes to copy.
	 */
	public static native void copy(final long p_addrDest, final long p_addrSrc, final long p_size);
	
	/**
	 * Read from main memory.
	 * @param p_addr Address to start reading from.
	 * @param p_array Target array to read data into.
	 * @param p_arrayOffset Startoffset in array.
	 * @param p_length Number of bytes to read.
	 */
	public static native void read(final long p_addr, byte[] p_array, final int p_arrayOffset, final int p_length);
	
	/**
	 * Write data from a byte array to main memory.
	 * @param p_addr Target startaddress to write to.
	 * @param p_array Array with data to write.
	 * @param p_arrayOffset Startoffset within array.
	 * @param p_length Number of bytes to write.
	 */
	public static native void write(final long p_addr, byte[] p_array, final int p_arrayOffset, final int p_length);

	/**
	 * Read a byte from memory.
	 * @param p_addr Address to read from.
	 * @return Read value.
	 */
	public static native byte readByte(final long p_addr);
	
	/**
	 * Read a short from memory.
	 * @param p_addr Address to read from.
	 * @return Read value.
	 */
	public static native short readShort(final long p_addr);
	
	/**
	 * Read an int from memory.
	 * @param p_addr Address to read from.
	 * @return Read value.
	 */
	public static native int readInt(final long p_addr);
	
	/**
	 * Read a long from memory.
	 * @param p_addr Address to read from.
	 * @return Read value.
	 */
	public static native long readLong(final long p_addr);
	
	/**
	 * Read a value with arbitrary length from memory (max. 8 bytes).
	 * @param p_addr Address to read from.
	 * @param p_byteCount Number of bytes to read into a long.
	 * @return Long value with number of bytes read from memory.
	 */
	public static native long readValue(final long p_addr, final int p_byteCount);
	
	/**
	 * Write a byte to memory.
	 * @param p_addr Address to write to.
	 * @param p_value Value to write.
	 */
	public static native void writeByte(final long p_addr, final byte p_value); 
	
	/**
	 * Write a short to memory.
	 * @param p_addr Address to write to.
	 * @param p_value Value to write.
	 */
	public static native void writeShort(final long p_addr, final short p_value);
	
	/**
	 * Write an int to memory.
	 * @param p_addr Address to write to.
	 * @param p_value Value to write.
	 */
	public static native void writeInt(final long p_addr, final int p_value);
	
	/**
	 * Write a long to memory.
	 * @param p_addr Address to write to.
	 * @param p_value Value to write.
	 */
	public static native void writeLong(final long p_addr, final long p_value);
	
	/**
	 * Write a value with arbitrary length to memory (max. 8 bytes).
	 * @param p_addr Address to write to.
	 * @param p_value Value to write.
	 * @param Number of bytes to write.
	 */
	public static native void writeValue(final long p_addr, final long p_value, final int p_byteCount);
	
}
