package de.uniduesseldorf.dxram.core.io;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.lookup.LookupHandler.Locations;
import de.uniduesseldorf.dxram.core.lookup.storage.OIDTree;
import de.uniduesseldorf.dxram.utils.Contract;

/**
 * Reads ChunkID, NodeID, Chunk and OIDTree from DataInput
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
		for (int i = 0;i < ret.length;i++) {
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
		for (int i = 0;i < ret.length;i++) {
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

		for (int i = 0;i < length;i++) {
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
		ByteBuffer data;

		Contract.checkNotNull(p_buffer, "no buffer given");

		chunkID = p_buffer.getLong();

		length = p_buffer.getInt();

		ret = new Chunk(chunkID, length);
		data = ret.getData();

		for (int i = 0;i < length;i++) {
			data.put(p_buffer.get());
		}

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
		for (int i = 0;i < ret.length;i++) {
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
		Chunk ret[];

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = new Chunk[p_buffer.getInt()];
		for (int i = 0;i < ret.length;i++) {
			ret[i] = readChunk(p_buffer);
		}

		return ret;
	}

	/**
	 * Reads an OIDTree from DataInput
	 * @param p_input
	 *            the DataInput
	 * @return the OIDTree
	 * @throws IOException
	 *             if the OIDTree could not be read
	 */
	public static OIDTree readOIDTree(final DataInput p_input) throws IOException {
		OIDTree ret = null;
		byte[] data;

		if (readBoolean(p_input)) {
			data = readByteArray(p_input);
			ret = parseOIDTree(data);
		}

		return ret;
	}

	public static OIDTree readOIDTree(final ByteBuffer p_buffer) {
		OIDTree ret = null;
		byte[] data;

		if (readBoolean(p_buffer)) {
			data = readByteArray(p_buffer);
			ret = parseOIDTree(data);
		}

		return ret;
	}

	public static OIDTree parseOIDTree(final byte[] p_data) {
		OIDTree ret = null;
		ByteArrayInputStream byteArrayInputStream;
		ObjectInput objectInput = null;

		if (p_data != null && p_data.length > 0) {
			byteArrayInputStream = new ByteArrayInputStream(p_data);
			try {
				objectInput = new ObjectInputStream(byteArrayInputStream);
				ret = (OIDTree)objectInput.readObject();
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

	public static boolean readBoolean(final DataInput p_input) throws IOException {
		Contract.checkNotNull(p_input, "no input given");

		return p_input.readBoolean();
	}

	public static boolean readBoolean(final ByteBuffer p_buffer) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		return p_buffer.get() == 1;
	}

	public static int readInt(final DataInput p_input) throws IOException {
		Contract.checkNotNull(p_input, "no input given");

		return p_input.readInt();
	}

	public static int readInt(final ByteBuffer p_buffer) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		return p_buffer.getInt();
	}

	public static byte readByte(final DataInput p_input) throws IOException {
		Contract.checkNotNull(p_input, "no input given");

		return p_input.readByte();
	}

	public static byte readByte(final ByteBuffer p_buffer) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		return p_buffer.get();
	}

	public static short readShort(final DataInput p_input) throws IOException {
		Contract.checkNotNull(p_input, "no input given");

		return p_input.readShort();
	}

	public static short readShort(final ByteBuffer p_buffer) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		return p_buffer.getShort();
	}

	public static long readLong(final DataInput p_input) throws IOException {
		Contract.checkNotNull(p_input, "no input given");

		return p_input.readLong();
	}

	public static long readLong(final ByteBuffer p_buffer) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		return p_buffer.getLong();
	}

	public static float readFloat(final DataInput p_input) throws IOException {
		Contract.checkNotNull(p_input, "no input given");

		return p_input.readFloat();
	}

	public static float readFloat(final ByteBuffer p_buffer) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		return p_buffer.getFloat();
	}

	public static double readDouble(final DataInput p_input) throws IOException {
		Contract.checkNotNull(p_input, "no input given");

		return p_input.readDouble();
	}

	public static double readDouble(final ByteBuffer p_buffer) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		return p_buffer.getDouble();
	}

	public static Locations readLocations(final DataInput p_input) throws IOException {
		Locations ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = new Locations(p_input.readLong(), new long[] {p_input.readLong(), p_input.readLong()});

		return ret;
	}

	public static Locations readLocations(final ByteBuffer p_buffer) {
		Locations ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = new Locations(p_buffer.getLong(), new long[] {p_buffer.getLong(), p_buffer.getLong()});

		return ret;
	}

	public static long[] readLongArray(final DataInput p_input) throws IOException {
		long[] ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = new long[p_input.readInt()];
		for (int i = 0;i < ret.length;i++) {
			ret[i] = p_input.readLong();
		}

		return ret;
	}

	public static long[] readLongArray(final ByteBuffer p_buffer) {
		long[] ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = new long[p_buffer.getInt()];
		for (int i = 0;i < ret.length;i++) {
			ret[i] = p_buffer.getLong();
		}

		return ret;
	}

	public static short[] readShortArray(final DataInput p_input) throws IOException {
		short[] ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = new short[p_input.readInt()];
		for (int i = 0;i < ret.length;i++) {
			ret[i] = p_input.readShort();
		}

		return ret;
	}

	public static short[] readShortArray(final ByteBuffer p_buffer) {
		short[] ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = new short[p_buffer.getInt()];
		for (int i = 0;i < ret.length;i++) {
			ret[i] = p_buffer.getShort();
		}

		return ret;
	}

	public static byte[] readByteArray(final DataInput p_input) throws IOException {
		byte[] ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = new byte[p_input.readInt()];
		for (int i = 0;i < ret.length;i++) {
			ret[i] = p_input.readByte();
		}

		return ret;
	}

	public static byte[] readByteArray(final ByteBuffer p_buffer) {
		byte[] ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = new byte[p_buffer.getInt()];
		for (int i = 0;i < ret.length;i++) {
			ret[i] = p_buffer.get();
		}

		return ret;
	}

	public static int[] readIntArray(final DataInput p_input) throws IOException {
		int[] ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = new int[p_input.readInt()];
		for (int i = 0;i < ret.length;i++) {
			ret[i] = p_input.readInt();
		}

		return ret;
	}

	public static int[] readIntArray(final ByteBuffer p_buffer) {
		int[] ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = new int[p_buffer.getInt()];
		for (int i = 0;i < ret.length;i++) {
			ret[i] = p_buffer.getInt();
		}

		return ret;
	}

	public static double[] readDoubleArray(final DataInput p_input) throws IOException {
		double[] ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = new double[p_input.readInt()];
		for (int i = 0;i < ret.length;i++) {
			ret[i] = p_input.readDouble();
		}

		return ret;
	}

	public static double[] readDoubleArray(final ByteBuffer p_buffer) {
		double[] ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = new double[p_buffer.getInt()];
		for (int i = 0;i < ret.length;i++) {
			ret[i] = p_buffer.getDouble();
		}

		return ret;
	}

	public static float[] readFloatArray(final DataInput p_input) throws IOException {
		float[] ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = new float[p_input.readInt()];
		for (int i = 0;i < ret.length;i++) {
			ret[i] = p_input.readFloat();
		}

		return ret;
	}

	public static float[] readFloatArray(final ByteBuffer p_buffer) {
		float[] ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = new float[p_buffer.getInt()];
		for (int i = 0;i < ret.length;i++) {
			ret[i] = p_buffer.getFloat();
		}

		return ret;
	}

	public static boolean[] readBooleanArray(final DataInput p_input) throws IOException {
		boolean[] ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = new boolean[p_input.readInt()];
		for (int i = 0;i < ret.length;i++) {
			ret[i] = readBoolean(p_input);
		}

		return ret;
	}

	public static boolean[] readBooleanArray(final ByteBuffer p_buffer) {
		boolean[] ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = new boolean[p_buffer.getInt()];
		for (int i = 0;i < ret.length;i++) {
			ret[i] = readBoolean(p_buffer);
		}

		return ret;
	}

}
