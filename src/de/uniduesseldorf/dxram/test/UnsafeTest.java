
package de.uniduesseldorf.dxram.test;

import java.nio.ByteBuffer;

import sun.misc.Unsafe;

import de.uniduesseldorf.dxram.core.mem.Chunk;

import de.uniduesseldorf.utils.unsafe.UnsafeHandler;

/**
 * Test case for the use of the Unsafe class
 * @author Florian Klein 10.07.2013
 */
public final class UnsafeTest {

	// Constants
	private static final long SIZE = 64L * 1024 * 1024;
	private static final int CHUNK_SIZE = 64 * 1024 * 1024;

	// Constructors
	/**
	 * Creates an instance of UnsafeTest
	 */
	private UnsafeTest() {}

	// Methods
	/**
	 * Program entry point
	 * @param p_arguments
	 *            The program arguments
	 */
	public static void main(final String[] p_arguments) {
		Unsafe unsafe;
		long address;
		long time;

		unsafe = UnsafeHandler.getInstance().getUnsafe();

		System.out.println("Allocate " + SIZE + " bytes of memory...");
		address = unsafe.allocateMemory(SIZE);
		System.out.println("Memory allocated - address: 0x" + Long.toHexString(address));

		System.out.print("Write data to memory");
		time = System.nanoTime();
		for (long i = address; i < address + SIZE; i++) {
			unsafe.putByte(i, (byte) 1);
		}
		time = System.nanoTime() - time;
		System.out.println(" - time: " + toString(time));

		System.out.print("Read data from memory");
		time = System.nanoTime();
		for (long i = address; i < address + SIZE; i++) {
			unsafe.getByte(i);
		}
		time = System.nanoTime() - time;
		System.out.println(" - time: " + toString(time));

		testChunk(unsafe, address, CHUNK_SIZE);

		System.out.println("Free memory...");
		unsafe.freeMemory(address);
		System.out.println("Memory freed");
	}

	/**
	 * Writes and reads a Chunk
	 * @param p_unsafe
	 *            the Unsafe instance
	 * @param p_address
	 *            the target address
	 * @param p_size
	 *            the size of the Chunk
	 */
	private static void testChunk(final Unsafe p_unsafe, final long p_address, final int p_size) {
		int size;
		Chunk writeChunk;
		Chunk readChunk;
		long time;

		size = p_size - 8;

		writeChunk = new Chunk(1, size);
		createData(writeChunk.getData());

		System.out.print("Write Chunk");
		time = System.nanoTime();
		write(p_unsafe, p_address, writeChunk);
		time = System.nanoTime() - time;
		System.out.println(" - time: " + toString(time));

		System.out.print("Read Chunk");
		time = System.nanoTime();
		readChunk = read(p_unsafe, p_address, size);
		time = System.nanoTime() - time;
		System.out.println(" - time: " + toString(time));

		if (writeChunk.getChunkID() != readChunk.getChunkID()) {
			System.out.println("wrong ID");
		}
		if (!compareData(writeChunk.getData(), readChunk.getData())) {
			System.out.println("wrong data");
		}
	}

	/**
	 * Creates random data to fill the given ByteBuffer
	 * @param p_buffer
	 *            the ByteBuffer to fill
	 */
	private static void createData(final ByteBuffer p_buffer) {
		for (int i = 0; i < p_buffer.capacity(); i++) {
			p_buffer.put((byte) (Math.random() * Byte.MAX_VALUE));
		}
	}

	/**
	 * Compares two ByteBuffers
	 * @param p_buffer1
	 *            the first ByteBuffer
	 * @param p_buffer2
	 *            the second ByteBuffer
	 * @return the result
	 */
	private static boolean compareData(final ByteBuffer p_buffer1, final ByteBuffer p_buffer2) {
		boolean ret = true;

		if (p_buffer1.capacity() == p_buffer2.capacity()) {
			for (int i = 0; i < p_buffer1.capacity(); i++) {
				p_buffer1.position(i);
				p_buffer2.position(i);
				if (p_buffer1.get() != p_buffer2.get()) {
					ret = false;

					break;
				}
			}
		} else {
			ret = false;
		}

		return ret;
	}

	/**
	 * Reads a Chunk from the given address
	 * @param p_unsafe
	 *            the unsafe instance
	 * @param p_address
	 *            the address to read from
	 * @param p_size
	 *            the size of the Chunk
	 * @return the read Chunk
	 */
	private static Chunk read(final Unsafe p_unsafe, final long p_address, final int p_size) {
		Chunk ret = null;
		long id;
		ByteBuffer data;

		id = p_unsafe.getLong(p_address);

		ret = new Chunk(id, p_size);
		data = ret.getData();

		for (long i = p_address + 8; i < p_address + 8 + p_size; i++) {
			data.put(p_unsafe.getByte(i));
		}

		return ret;
	}

	/**
	 * Writes a CHunk to the given address
	 * @param p_unsafe
	 *            the unsafe instance
	 * @param p_address
	 *            the address to write to
	 * @param p_chunk
	 *            the chunk to write
	 */
	private static void write(final Unsafe p_unsafe, final long p_address, final Chunk p_chunk) {
		ByteBuffer data;

		p_unsafe.putLong(p_address, p_chunk.getChunkID());

		data = p_chunk.getData();
		data.position(0);
		for (long i = p_address + 8; i < p_address + 8 + p_chunk.getSize(); i++) {
			p_unsafe.putByte(i, data.get());
		}
	}

	/**
	 * Converts a time value to a String
	 * @param p_time
	 *            the time value
	 * @return the String
	 */
	private static String toString(final long p_time) {
		long time;
		long nanoseconds;
		long microseconds;
		long milliseconds;
		long seconds;

		time = p_time;

		nanoseconds = time % 1000;
		time = time / 1000;

		microseconds = time % 1000;
		time = time / 1000;

		milliseconds = time % 1000;
		time = time / 1000;

		seconds = time % 60;
		time = time / 60;

		return seconds + "s " + milliseconds + "ms " + microseconds + "µs " + nanoseconds + "ns";
	}

}
