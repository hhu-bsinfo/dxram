
package de.uniduesseldorf.dxram.core.io;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.chunk.ChunkHandler.BackupRange;
import de.uniduesseldorf.dxram.core.lookup.LookupHandler.Locations;
import de.uniduesseldorf.dxram.core.lookup.storage.LookupTree;
import de.uniduesseldorf.dxram.utils.Contract;

/**
 * Reads data types
 * @author Florian Klein 26.03.2012
 */
public final class InputHelper {

	// Constructors
	/**
	 * Creates an instance of DXRAMDataInputHelper
	 */
	private InputHelper() {}

	// Methods
	/**
	 * Reads a ChunkID from DataInput
	 * @param p_input
	 *            the DataInput
	 * @return the read ChunkID
	 * @throws IOException
	 *             if the ChunkID could not be read
	 */
	public static long readChunkID(final DataInput p_input) throws IOException {
		long ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = p_input.readLong();

		return ret;
	}

	/**
	 * Reads a ChunkID from ByteBuffer
	 * @param p_buffer
	 *            the ByteBuffer
	 * @return the read ChunkID
	 */
	public static long readChunkID(final ByteBuffer p_buffer) {
		long ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = p_buffer.getLong();

		return ret;
	}

	/**
	 * Reads ChunkIDs from DataInput
	 * @param p_input
	 *            the DataInput
	 * @return the read ChunkIDs
	 * @throws IOException
	 *             if the ChunkIDs could not be read
	 */
	public static long[] readChunkIDs(final DataInput p_input) throws IOException {
		long[] ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = new long[p_input.readInt()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = readChunkID(p_input);
		}

		return ret;
	}

	/**
	 * Reads ChunkIDs from ByteBuffer
	 * @param p_buffer
	 *            the ByteBuffer
	 * @return the read ChunkIDs
	 */
	public static long[] readChunkIDs(final ByteBuffer p_buffer) {
		long[] ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = new long[p_buffer.getInt()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = readChunkID(p_buffer);
		}

		return ret;
	}

	/**
	 * Reads a NodeID from DataInput
	 * @param p_input
	 *            the DataInput
	 * @return the read NodeID
	 * @throws IOException
	 *             if the NodeID could not be read
	 */
	public static short readNodeID(final DataInput p_input) throws IOException {
		short ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = p_input.readShort();

		return ret;
	}

	/**
	 * Reads a NodeID from ByteBuffer
	 * @param p_buffer
	 *            the ByteBuffer
	 * @return the read NodeID
	 */
	public static short readNodeID(final ByteBuffer p_buffer) {
		short ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = p_buffer.getShort();

		return ret;
	}

	/**
	 * Reads a Chunk from DataInput
	 * @param p_input
	 *            the DataInput
	 * @return the read Chunk
	 * @throws IOException
	 *             if the Chunk could not be read
	 */
	public static Chunk readChunk(final DataInput p_input) throws IOException {
		Chunk ret = null;
		long chunkID;
		int length;
		ByteBuffer data;

		chunkID = p_input.readLong();

		length = p_input.readInt();

		ret = new Chunk(chunkID, length);
		data = ret.getData();

		for (int i = 0; i < length; i++) {
			data.put(p_input.readByte());
		}

		return ret;
	}

	/**
	 * Reads a Chunk from ByteBuffer
	 * @param p_buffer
	 *            the ByteBuffer
	 * @return the read Chunk
	 */
	public static Chunk readChunk(final ByteBuffer p_buffer) {
		Chunk ret;
		long chunkID;
		int length;
		byte[] data;

		Contract.checkNotNull(p_buffer, "no buffer given");

		chunkID = p_buffer.getLong();
		length = p_buffer.getInt();
		data = new byte[length];
		p_buffer.get(data, 0, length);

		ret = new Chunk(chunkID, data);

		return ret;
	}

	/**
	 * Reads Chunks from DataInput
	 * @param p_input
	 *            the DataInput
	 * @return the read Chunks
	 * @throws IOException
	 *             if the Chunks could not be read
	 */
	public static Chunk[] readChunks(final DataInput p_input) throws IOException {
		Chunk[] ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = new Chunk[p_input.readInt()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = readChunk(p_input);
		}

		return ret;
	}

	/**
	 * Reads Chunks from ByteBuffer
	 * @param p_buffer
	 *            the ByteBuffer
	 * @return the read Chunks
	 */
	public static Chunk[] readChunks(final ByteBuffer p_buffer) {
		Chunk[] ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = new Chunk[p_buffer.getInt()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = readChunk(p_buffer);
		}

		return ret;
	}

	/**
	 * Reads an CIDTree from DataInput
	 * @param p_input
	 *            the DataInput
	 * @return the CIDTree
	 * @throws IOException
	 *             if the CIDTree could not be read
	 */
	public static LookupTree readCIDTree(final DataInput p_input) throws IOException {
		LookupTree ret = null;
		byte[] data;

		if (readBoolean(p_input)) {
			data = readByteArray(p_input);
			ret = parseCIDTree(data);
		}

		return ret;
	}

	/**
	 * Reads an CIDTree from ByteBuffer
	 * @param p_buffer
	 *            the ByteBuffer
	 * @return the CIDTree
	 */
	public static LookupTree readCIDTree(final ByteBuffer p_buffer) {
		LookupTree ret = null;
		byte[] data;

		if (readBoolean(p_buffer)) {
			data = readByteArray(p_buffer);
			ret = parseCIDTree(data);
		}

		return ret;
	}

	/**
	 * Parses binary data into an CIDTree
	 * @param p_data
	 *            the binary data
	 * @return the CIDTree
	 */
	public static LookupTree parseCIDTree(final byte[] p_data) {
		LookupTree ret = null;
		ByteArrayInputStream byteArrayInputStream;
		ObjectInput objectInput = null;

		if (p_data != null && p_data.length > 0) {
			byteArrayInputStream = new ByteArrayInputStream(p_data);
			try {
				objectInput = new ObjectInputStream(byteArrayInputStream);
				ret = (LookupTree) objectInput.readObject();
			} catch (final Exception e) {} finally {
				try {
					if (objectInput != null) {
						objectInput.close();
					}
					byteArrayInputStream.close();
				} catch (final IOException e) {}
			}
		}

		return ret;
	}

	/**
	 * Reads a boolean
	 * @param p_input
	 *            the input
	 * @return the boolean
	 * @throws IOException
	 *             if the boolean could not be read
	 */
	public static boolean readBoolean(final DataInput p_input) throws IOException {
		Contract.checkNotNull(p_input, "no input given");

		return p_input.readBoolean();
	}

	/**
	 * Reads a boolean
	 * @param p_buffer
	 *            the buffer
	 * @return the boolean
	 */
	public static boolean readBoolean(final ByteBuffer p_buffer) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		return p_buffer.get() == 1;
	}

	/**
	 * Reads an integer
	 * @param p_input
	 *            the input
	 * @return the integer
	 * @throws IOException
	 *             if the integer could not be read
	 */
	public static int readInt(final DataInput p_input) throws IOException {
		Contract.checkNotNull(p_input, "no input given");

		return p_input.readInt();
	}

	/**
	 * Reads an integer
	 * @param p_buffer
	 *            the buffer
	 * @return the integer
	 */
	public static int readInt(final ByteBuffer p_buffer) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		return p_buffer.getInt();
	}

	/**
	 * Reads a byte
	 * @param p_input
	 *            the input
	 * @return the byte
	 * @throws IOException
	 *             if the byte could not be read
	 */
	public static byte readByte(final DataInput p_input) throws IOException {
		Contract.checkNotNull(p_input, "no input given");

		return p_input.readByte();
	}

	/**
	 * Reads a byte
	 * @param p_buffer
	 *            the buffer
	 * @return the byte
	 */
	public static byte readByte(final ByteBuffer p_buffer) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		return p_buffer.get();
	}

	/**
	 * Reads a short
	 * @param p_input
	 *            the input
	 * @return the short
	 * @throws IOException
	 *             if the short could not be read
	 */
	public static short readShort(final DataInput p_input) throws IOException {
		Contract.checkNotNull(p_input, "no input given");

		return p_input.readShort();
	}

	/**
	 * Reads a short
	 * @param p_buffer
	 *            the buffer
	 * @return the short
	 */
	public static short readShort(final ByteBuffer p_buffer) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		return p_buffer.getShort();
	}

	/**
	 * Reads a long
	 * @param p_input
	 *            the input
	 * @return the long
	 * @throws IOException
	 *             if the long could not be read
	 */
	public static long readLong(final DataInput p_input) throws IOException {
		Contract.checkNotNull(p_input, "no input given");

		return p_input.readLong();
	}

	/**
	 * Reads a long
	 * @param p_buffer
	 *            the buffer
	 * @return the long
	 */
	public static long readLong(final ByteBuffer p_buffer) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		return p_buffer.getLong();
	}

	/**
	 * Reads a float
	 * @param p_input
	 *            the input
	 * @return the float
	 * @throws IOException
	 *             if the float could not be read
	 */
	public static float readFloat(final DataInput p_input) throws IOException {
		Contract.checkNotNull(p_input, "no input given");

		return p_input.readFloat();
	}

	/**
	 * Reads a float
	 * @param p_buffer
	 *            the buffer
	 * @return the float
	 */
	public static float readFloat(final ByteBuffer p_buffer) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		return p_buffer.getFloat();
	}

	/**
	 * Reads a double
	 * @param p_input
	 *            the input
	 * @return the double
	 * @throws IOException
	 *             if the double could not be read
	 */
	public static double readDouble(final DataInput p_input) throws IOException {
		Contract.checkNotNull(p_input, "no input given");

		return p_input.readDouble();
	}

	/**
	 * Reads a double
	 * @param p_buffer
	 *            the buffer
	 * @return the double
	 */
	public static double readDouble(final ByteBuffer p_buffer) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		return p_buffer.getDouble();
	}

	/**
	 * Reads a String
	 * @param p_input
	 *            the input
	 * @return the String
	 * @throws IOException
	 *             if the long could not be read
	 */
	public static String readString(final DataInput p_input) throws IOException {
		short length;
		byte[] byteArray;

		Contract.checkNotNull(p_input, "no input given");

		length = p_input.readShort();
		byteArray = new byte[length];

		for (int i = 0; i < length; i++) {
			byteArray[i] = p_input.readByte();
		}

		return new String(byteArray);
	}

	/**
	 * Reads a String
	 * @param p_buffer
	 *            the buffer
	 * @return the String
	 */
	public static String readString(final ByteBuffer p_buffer) {
		short length;
		byte[] byteArray;

		Contract.checkNotNull(p_buffer, "no buffer given");

		length = p_buffer.getShort();
		byteArray = new byte[length];
		p_buffer.get(byteArray, 0, length);

		return new String(byteArray);
	}

	/**
	 * Reads locations
	 * @param p_input
	 *            the input
	 * @return the locations
	 * @throws IOException
	 *             if the locations could not be read
	 */
	public static Locations readLocations(final DataInput p_input) throws IOException {
		Locations ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = new Locations(p_input.readLong(), new long[] {p_input.readLong(), p_input.readLong()});

		return ret;
	}

	/**
	 * Reads locations
	 * @param p_buffer
	 *            the buffer
	 * @return the locations
	 */
	public static Locations readLocations(final ByteBuffer p_buffer) {
		Locations ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = new Locations(p_buffer.getLong(), new long[] {p_buffer.getLong(), p_buffer.getLong()});

		return ret;
	}

	/**
	 * Reads a BackupRange
	 * @param p_input
	 *            the input
	 * @return the BackupRange
	 * @throws IOException
	 *             if the locations could not be read
	 */
	public static BackupRange readBackupRange(final DataInput p_input) throws IOException {
		BackupRange ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = new BackupRange(p_input.readLong(), p_input.readLong());

		return ret;
	}

	/**
	 * Reads a BackupRange
	 * @param p_buffer
	 *            the buffer
	 * @return the BackupRange
	 */
	public static BackupRange readBackupRange(final ByteBuffer p_buffer) {
		BackupRange ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = new BackupRange(p_buffer.getLong(), p_buffer.getLong());

		return ret;
	}

	/**
	 * Reads a long array
	 * @param p_input
	 *            the input
	 * @return the long array
	 * @throws IOException
	 *             if the long array could not be read
	 */
	public static long[] readLongArray(final DataInput p_input) throws IOException {
		long[] ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = new long[p_input.readInt()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = p_input.readLong();
		}

		return ret;
	}

	/**
	 * Reads a long array
	 * @param p_buffer
	 *            the buffer
	 * @return the long array
	 */
	public static long[] readLongArray(final ByteBuffer p_buffer) {
		long[] ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = new long[p_buffer.getInt()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = p_buffer.getLong();
		}

		return ret;
	}

	/**
	 * Reads a short array
	 * @param p_input
	 *            the input
	 * @return the short array
	 * @throws IOException
	 *             if the short array could not be read
	 */
	public static short[] readShortArray(final DataInput p_input) throws IOException {
		short[] ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = new short[p_input.readInt()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = p_input.readShort();
		}

		return ret;
	}

	/**
	 * Reads a short array
	 * @param p_buffer
	 *            the buffer
	 * @return the short array
	 */
	public static short[] readShortArray(final ByteBuffer p_buffer) {
		short[] ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = new short[p_buffer.getInt()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = p_buffer.getShort();
		}

		return ret;
	}

	/**
	 * Reads a byte array
	 * @param p_input
	 *            the input
	 * @return the byte array
	 * @throws IOException
	 *             if the byte array could not be read
	 */
	public static byte[] readByteArray(final DataInput p_input) throws IOException {
		byte[] ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = new byte[p_input.readInt()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = p_input.readByte();
		}

		return ret;
	}

	/**
	 * Reads a byte array
	 * @param p_buffer
	 *            the buffer
	 * @return the byte array
	 */
	public static byte[] readByteArray(final ByteBuffer p_buffer) {
		byte[] ret;
		int length;

		Contract.checkNotNull(p_buffer, "no buffer given");

		length = p_buffer.getInt();
		ret = new byte[length];
		p_buffer.get(ret, 0, length);

		return ret;
	}

	/**
	 * Reads a integer array
	 * @param p_input
	 *            the input
	 * @return the integer array
	 * @throws IOException
	 *             if the integer array could not be read
	 */
	public static int[] readIntArray(final DataInput p_input) throws IOException {
		int[] ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = new int[p_input.readInt()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = p_input.readInt();
		}

		return ret;
	}

	/**
	 * Reads a integer array
	 * @param p_buffer
	 *            the buffer
	 * @return the integer array
	 */
	public static int[] readIntArray(final ByteBuffer p_buffer) {
		int[] ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = new int[p_buffer.getInt()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = p_buffer.getInt();
		}

		return ret;
	}

	/**
	 * Reads a double array
	 * @param p_input
	 *            the input
	 * @return the double array
	 * @throws IOException
	 *             if the double array could not be read
	 */
	public static double[] readDoubleArray(final DataInput p_input) throws IOException {
		double[] ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = new double[p_input.readInt()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = p_input.readDouble();
		}

		return ret;
	}

	/**
	 * Reads a double array
	 * @param p_buffer
	 *            the buffer
	 * @return the double array
	 */
	public static double[] readDoubleArray(final ByteBuffer p_buffer) {
		double[] ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = new double[p_buffer.getInt()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = p_buffer.getDouble();
		}

		return ret;
	}

	/**
	 * Reads a float array
	 * @param p_input
	 *            the input
	 * @return the float array
	 * @throws IOException
	 *             if the float array could not be read
	 */
	public static float[] readFloatArray(final DataInput p_input) throws IOException {
		float[] ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = new float[p_input.readInt()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = p_input.readFloat();
		}

		return ret;
	}

	/**
	 * Reads a float array
	 * @param p_buffer
	 *            the buffer
	 * @return the float array
	 */
	public static float[] readFloatArray(final ByteBuffer p_buffer) {
		float[] ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = new float[p_buffer.getInt()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = p_buffer.getFloat();
		}

		return ret;
	}

	/**
	 * Reads a boolean array
	 * @param p_input
	 *            the input
	 * @return the boolean array
	 * @throws IOException
	 *             if the boolean array could not be read
	 */
	public static boolean[] readBooleanArray(final DataInput p_input) throws IOException {
		boolean[] ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = new boolean[p_input.readInt()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = readBoolean(p_input);
		}

		return ret;
	}

	/**
	 * Reads a boolean array
	 * @param p_buffer
	 *            the buffer
	 * @return the boolean array
	 */
	public static boolean[] readBooleanArray(final ByteBuffer p_buffer) {
		boolean[] ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = new boolean[p_buffer.getInt()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = readBoolean(p_buffer);
		}

		return ret;
	}

	/**
	 * Reads a String array
	 * @param p_input
	 *            the input
	 * @return the String array
	 * @throws IOException
	 *             if the String array could not be read
	 */
	public static String[] readStringArray(final DataInput p_input) throws IOException {
		String[] ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = new String[p_input.readInt()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = readString(p_input);
		}

		return ret;
	}

	/**
	 * Reads a String array
	 * @param p_buffer
	 *            the buffer
	 * @return the String array
	 */
	public static String[] readStringArray(final ByteBuffer p_buffer) {
		String[] ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = new String[p_buffer.getInt()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = readString(p_buffer);
		}

		return ret;
	}

}
