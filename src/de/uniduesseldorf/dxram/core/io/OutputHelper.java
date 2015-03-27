package de.uniduesseldorf.dxram.core.io;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.lookup.LookupHandler.Locations;
import de.uniduesseldorf.dxram.core.lookup.storage.OIDTree;
import de.uniduesseldorf.dxram.utils.Contract;

/**
 * Writes data types
 * @author Florian Klein 26.03.2012
 */
public final class OutputHelper {

	// Constructors
	/**
	 * Creates an instance of DXRAMDataOutputHelper
	 */
	private OutputHelper() {}

	// Methods
	/**
	 * Get the length of a ChunkID
	 * @return the length of a ChunkID
	 */
	public static int getChunkIDWriteLength() {
		return 8;
	}

	/**
	 * Writes a ChunkID to DataOutput
	 * @param p_output
	 *            the DataOutput
	 * @param p_chunkID
	 *            the ChunkID
	 * @throws IOException
	 *             if the ChunkID could not be written
	 */
	public static void writeChunkID(final DataOutput p_output, final long p_chunkID) throws IOException {
		Contract.checkNotNull(p_output, "no output given");

		p_output.writeLong(p_chunkID);
	}

	/**
	 * Writes a ChunkID to ByteBuffer
	 * @param p_buffer
	 *            the ByteBuffer
	 * @param p_chunkID
	 *            the ChunkID
	 */
	public static void writeChunkID(final ByteBuffer p_buffer, final long p_chunkID) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		p_buffer.putLong(p_chunkID);
	}

	/**
	 * Get the length of multiple ChunkIDs
	 * @param p_count
	 *            the number of ChunkIDs
	 * @return the length of the ChunkIDs
	 */
	public static int getChunkIDsWriteLength(final int p_count) {
		return getChunkIDWriteLength() * p_count + 4;
	}

	/**
	 * Writes ChunkIDs to DataOutput
	 * @param p_output
	 *            the DataOutput
	 * @param p_chunkIDs
	 *            the ChunkIDs
	 * @throws IOException
	 *             if the ChunkIDs could not be written
	 */
	public static void writeChunkIDs(final DataOutput p_output, final long[] p_chunkIDs) throws IOException {
		Contract.checkNotNull(p_output, "no output given");
		Contract.checkNotNull(p_chunkIDs, "no ChunkIDs given");

		p_output.writeInt(p_chunkIDs.length);
		for (long chunkID : p_chunkIDs) {
			writeChunkID(p_output, chunkID);
		}
	}

	/**
	 * Writes ChunkIDs to ByteBuffer
	 * @param p_buffer
	 *            the ByteBuffer
	 * @param p_chunkIDs
	 *            the ChunkIDs
	 */
	public static void writeChunkIDs(final ByteBuffer p_buffer, final long[] p_chunkIDs) {
		Contract.checkNotNull(p_buffer, "no buffer given");
		Contract.checkNotNull(p_chunkIDs, "no ChunkIDs given");

		p_buffer.putInt(p_chunkIDs.length);
		for (long chunkID : p_chunkIDs) {
			writeChunkID(p_buffer, chunkID);
		}
	}

	/**
	 * Get the length of a NodeID
	 * @return the length of a NodeID
	 */
	public static int getNodeIDWriteLength() {
		return 2;
	}

	/**
	 * Writes a NodeID to DataOutput
	 * @param p_output
	 *            the DataOutput
	 * @param p_nodeID
	 *            the NodeID
	 * @throws IOException
	 *             if the NodeID could not be written
	 */
	public static void writeNodeID(final DataOutput p_output, final short p_nodeID) throws IOException {
		Contract.checkNotNull(p_output, "no output given");

		p_output.writeShort(p_nodeID);
	}

	/**
	 * Writes a NodeID to ByteBuffer
	 * @param p_buffer
	 *            the ByteBuffer
	 * @param p_nodeID
	 *            the NodeID
	 */
	public static void writeNodeID(final ByteBuffer p_buffer, final short p_nodeID) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		p_buffer.putShort(p_nodeID);
	}

	/**
	 * Get the length of a Chunk
	 * @param p_chunk
	 *            the Chunk
	 * @return the length of the Chunk
	 */
	public static int getChunkWriteLength(final Chunk p_chunk) {
		Contract.checkNotNull(p_chunk, "no chunk given");

		return 12 + p_chunk.getSize();
	}

	/**
	 * Writes a Chunk to DataOutput
	 * @param p_output
	 *            the DataOutput
	 * @param p_chunk
	 *            the Chunk
	 * @throws IOException
	 *             if the Chunk could not be written
	 */
	public static void writeChunk(final DataOutput p_output, final Chunk p_chunk) throws IOException {
		Contract.checkNotNull(p_output, "no output given");
		Contract.checkNotNull(p_chunk, "no chunk given");

		p_output.writeLong(p_chunk.getChunkID());
		p_output.writeInt(p_chunk.getSize());
		p_output.write(p_chunk.getData().array());
	}

	/**
	 * Writes a Chunk to ByteBuffer
	 * @param p_buffer
	 *            the ByteBuffer
	 * @param p_chunk
	 *            the Chunk
	 */
	public static void writeChunk(final ByteBuffer p_buffer, final Chunk p_chunk) {
		Contract.checkNotNull(p_buffer, "no buffer given");
		Contract.checkNotNull(p_chunk, "no chunk given");

		p_buffer.putLong(p_chunk.getChunkID());
		p_buffer.putInt(p_chunk.getSize());
		p_buffer.put(p_chunk.getData());
	}

	/**
	 * Get the length of multiple Chunks
	 * @param p_chunks
	 *            the Chunks
	 * @return the length of the Chunk
	 */
	public static int getChunksWriteLength(final Chunk[] p_chunks) {
		int ret;

		Contract.checkNotNull(p_chunks, "no chunks given");

		ret = 4;
		for (Chunk chunk : p_chunks) {
			ret += getChunkWriteLength(chunk);
		}

		return ret;
	}

	/**
	 * Writes Chunks to DataOutput
	 * @param p_output
	 *            the DataOutput
	 * @param p_chunks
	 *            the Chunks
	 * @throws IOException
	 *             if the Chunks could not be written
	 */
	public static void writeChunks(final DataOutput p_output, final Chunk[] p_chunks) throws IOException {
		Contract.checkNotNull(p_output, "no output given");
		Contract.checkNotNull(p_chunks, "no chunks given");

		p_output.writeInt(p_chunks.length);
		for (Chunk chunk : p_chunks) {
			writeChunk(p_output, chunk);
		}
	}

	/**
	 * Writes Chunks to ByteBuffer
	 * @param p_buffer
	 *            the ByteBuffer
	 * @param p_chunks
	 *            the Chunks
	 */
	public static void writeChunks(final ByteBuffer p_buffer, final Chunk[] p_chunks) {
		Contract.checkNotNull(p_buffer, "no buffer given");
		Contract.checkNotNull(p_chunks, "no chunks given");

		p_buffer.putInt(p_chunks.length);
		for (Chunk chunk : p_chunks) {
			writeChunk(p_buffer, chunk);
		}
	}

	/**
	 * Get the lenth of a OIDTree
	 * @param p_tree
	 *            the OIDTree
	 * @return the lenght of the OIDTree
	 */
	public static int getOIDTreeWriteLength(final OIDTree p_tree) {
		int ret;
		byte[] data;

		if (p_tree == null) {
			ret = getBooleanWriteLength();
		} else {
			data = parseOIDTree(p_tree);
			ret = getBooleanWriteLength();
			if (data != null) {
				ret += getByteArrayWriteLength(data.length);
			}
		}

		return ret;
	}

	/**
	 * Writes an OIDTree to DataOutput
	 * @param p_output
	 *            the DataOutput
	 * @param p_tree
	 *            the OIDTree
	 * @throws IOException
	 *             if the OIDTree could not be written
	 */
	public static void writeOIDTree(final DataOutput p_output, final OIDTree p_tree) throws IOException {
		byte[] data;

		Contract.checkNotNull(p_output, "no output given");

		if (p_tree == null) {
			writeBoolean(p_output, false);
		} else {
			data = parseOIDTree(p_tree);
			writeBoolean(p_output, data != null);
			if (data != null) {
				writeByteArray(p_output, data);
			}
		}
	}

	/**
	 * Writes an OIDTree
	 * @param p_buffer
	 *            the buffer
	 * @param p_tree
	 *            the OIDTree
	 */
	public static void writeOIDTree(final ByteBuffer p_buffer, final OIDTree p_tree) {
		byte[] data;

		Contract.checkNotNull(p_buffer, "no buffer given");

		if (p_tree == null) {
			writeBoolean(p_buffer, false);
		} else {
			data = parseOIDTree(p_tree);
			writeBoolean(p_buffer, data != null);
			if (data != null) {
				writeByteArray(p_buffer, data);
			}
		}
	}

	/**
	 * Parses an OIDTree to a byte array
	 * @param p_tree
	 *            the OIDTree
	 * @return the byte array
	 */
	private static byte[] parseOIDTree(final OIDTree p_tree) {
		byte[] ret = null;
		ByteArrayOutputStream byteArrayOutputStream;
		ObjectOutput objectOutput = null;

		byteArrayOutputStream = new ByteArrayOutputStream();
		try {
			objectOutput = new ObjectOutputStream(byteArrayOutputStream);
			objectOutput.writeObject(p_tree);
			ret = byteArrayOutputStream.toByteArray();
		} catch (final IOException e) {} finally {
			try {
				if (objectOutput != null) {
					objectOutput.close();
				}
				byteArrayOutputStream.close();
			} catch (final IOException e) {}
		}

		return ret;
	}

	/**
	 * Get the length of a boolean
	 * @return the length of a boolean
	 */
	public static int getBooleanWriteLength() {
		return 1;
	}

	/**
	 * Writes a boolean
	 * @param p_output
	 *            the output
	 * @param p_boolean
	 *            the boolean
	 * @throws IOException
	 *             if the boolean could not be written
	 */
	public static void writeBoolean(final DataOutput p_output, final boolean p_boolean) throws IOException {
		Contract.checkNotNull(p_output, "no output given");

		p_output.writeBoolean(p_boolean);
	}

	/**
	 * Writes a boolean
	 * @param p_buffer
	 *            the buffer
	 * @param p_boolean
	 *            the boolean
	 */
	public static void writeBoolean(final ByteBuffer p_buffer, final boolean p_boolean) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		if (p_boolean) {
			p_buffer.put((byte)1);
		} else {
			p_buffer.put((byte)0);
		}
	}

	/**
	 * Get the length of an integer
	 * @return the length of an integer
	 */
	public static int getIntWriteLength() {
		return 4;
	}

	/**
	 * Writes an integer
	 * @param p_output
	 *            the output
	 * @param p_int
	 *            the integer
	 * @throws IOException
	 *             if the integer could not be written
	 */
	public static void writeInt(final DataOutput p_output, final int p_int) throws IOException {
		Contract.checkNotNull(p_output, "no output given");

		p_output.writeInt(p_int);
	}

	/**
	 * Writes a integer
	 * @param p_buffer
	 *            the buffer
	 * @param p_int
	 *            the integer
	 */
	public static void writeInt(final ByteBuffer p_buffer, final int p_int) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		p_buffer.putInt(p_int);
	}

	/**
	 * Get the length of a byte
	 * @return the length of a byte
	 */
	public static int getByteWriteLength() {
		return 1;
	}

	/**
	 * Writes a byte
	 * @param p_output
	 *            the output
	 * @param p_byte
	 *            the byte
	 * @throws IOException
	 *             if the byte could not be written
	 */
	public static void writeByte(final DataOutput p_output, final byte p_byte) throws IOException {
		Contract.checkNotNull(p_output, "no output given");

		p_output.writeByte(p_byte);
	}

	/**
	 * Writes a byte
	 * @param p_buffer
	 *            the buffer
	 * @param p_byte
	 *            the byte
	 */
	public static void writeByte(final ByteBuffer p_buffer, final byte p_byte) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		p_buffer.put(p_byte);
	}

	/**
	 * Get the length of a short
	 * @return the length of a short
	 */
	public static int getShortWriteLength() {
		return 2;
	}

	/**
	 * Writes a short
	 * @param p_output
	 *            the output
	 * @param p_short
	 *            the short
	 * @throws IOException
	 *             if the short could not be written
	 */
	public static void writeShort(final DataOutput p_output, final short p_short) throws IOException {
		Contract.checkNotNull(p_output, "no output given");

		p_output.writeShort(p_short);
	}

	/**
	 * Writes a short
	 * @param p_buffer
	 *            the buffer
	 * @param p_short
	 *            the short
	 */
	public static void writeShort(final ByteBuffer p_buffer, final short p_short) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		p_buffer.putShort(p_short);
	}

	/**
	 * Get the length of a long
	 * @return the length of a long
	 */
	public static int getLongWriteLength() {
		return 8;
	}

	/**
	 * Writes a long
	 * @param p_output
	 *            the output
	 * @param p_long
	 *            the long
	 * @throws IOException
	 *             if the long could not be written
	 */
	public static void writeLong(final DataOutput p_output, final long p_long) throws IOException {
		Contract.checkNotNull(p_output, "no output given");

		p_output.writeLong(p_long);
	}

	/**
	 * Writes a long
	 * @param p_buffer
	 *            the buffer
	 * @param p_long
	 *            the long
	 */
	public static void writeLong(final ByteBuffer p_buffer, final long p_long) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		p_buffer.putLong(p_long);
	}

	/**
	 * Get the length of a float
	 * @return the length of a float
	 */
	public static int getFloatWriteLength() {
		return 4;
	}

	/**
	 * Writes a float
	 * @param p_output
	 *            the output
	 * @param p_float
	 *            the float
	 * @throws IOException
	 *             if the float could not be written
	 */
	public static void writeFloat(final DataOutput p_output, final float p_float) throws IOException {
		Contract.checkNotNull(p_output, "no output given");

		p_output.writeFloat(p_float);
	}

	/**
	 * Writes a float
	 * @param p_buffer
	 *            the buffer
	 * @param p_float
	 *            the float
	 */
	public static void writeFloat(final ByteBuffer p_buffer, final float p_float) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		p_buffer.putFloat(p_float);
	}

	/**
	 * Get the length of a double
	 * @return the length of a double
	 */
	public static int getDoubleWriteLength() {
		return 8;
	}

	/**
	 * Writes a double
	 * @param p_output
	 *            the output
	 * @param p_double
	 *            the double
	 * @throws IOException
	 *             if the double could not be written
	 */
	public static void writeDouble(final DataOutput p_output, final double p_double) throws IOException {
		Contract.checkNotNull(p_output, "no output given");

		p_output.writeDouble(p_double);
	}

	/**
	 * Writes a double
	 * @param p_buffer
	 *            the buffer
	 * @param p_double
	 *            the double
	 */
	public static void writeDouble(final ByteBuffer p_buffer, final double p_double) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		p_buffer.putDouble(p_double);
	}

	/**
	 * Get the length of a boolean
	 * @return the length of a boolean
	 */
	public static int getLocationsWriteLength() {
		return 24;
	}

	/**
	 * Writes a location
	 * @param p_output
	 *            the output
	 * @param p_locations
	 *            the location
	 * @throws IOException
	 *             if the location could not be written
	 */
	public static void writeLocations(final DataOutput p_output, final Locations p_locations) throws IOException {
		Contract.checkNotNull(p_output, "no output given");
		Contract.checkNotNull(p_locations, "no locations given");

		p_output.writeLong(p_locations.convertToLong());
		p_output.writeLong(p_locations.getStartID());
		p_output.writeLong(p_locations.getEndID());
	}

	/**
	 * Writes a location
	 * @param p_buffer
	 *            the buffer
	 * @param p_locations
	 *            the location
	 */
	public static void writeLocations(final ByteBuffer p_buffer, final Locations p_locations) {
		Contract.checkNotNull(p_buffer, "no buffer given");
		Contract.checkNotNull(p_locations, "no locations given");

		p_buffer.putLong(p_locations.convertToLong());
		p_buffer.putLong(p_locations.getStartID());
		p_buffer.putLong(p_locations.getEndID());
	}

	/**
	 * Get the length of multiple longs
	 * @param p_count
	 *            the number of longs
	 * @return the length of the longs
	 */
	public static int getLongArrayWriteLength(final int p_count) {
		return 8 * p_count + 4;
	}

	/**
	 * Writes a long array
	 * @param p_output
	 *            the output
	 * @param p_array
	 *            the array
	 * @throws IOException
	 *             if the array could not be written
	 */
	public static void writeLongArray(final DataOutput p_output, final long[] p_array) throws IOException {
		Contract.checkNotNull(p_output, "no output given");
		Contract.checkNotNull(p_array, "no array given");

		p_output.writeInt(p_array.length);
		for (long value : p_array) {
			p_output.writeLong(value);
		}
	}

	/**
	 * Writes a long array
	 * @param p_buffer
	 *            the buffer
	 * @param p_array
	 *            the array
	 */
	public static void writeLongArray(final ByteBuffer p_buffer, final long[] p_array) {
		Contract.checkNotNull(p_buffer, "no buffer given");
		Contract.checkNotNull(p_array, "no array given");

		p_buffer.putInt(p_array.length);
		for (long value : p_array) {
			p_buffer.putLong(value);
		}
	}

	/**
	 * Get the length of multiple shorts
	 * @param p_count
	 *            the number of shorts
	 * @return the length of the shorts
	 */
	public static int getShortArrayWriteLength(final int p_count) {
		return 2 * p_count + 4;
	}

	/**
	 * Writes a short array
	 * @param p_output
	 *            the output
	 * @param p_array
	 *            the array
	 * @throws IOException
	 *             if the array could not be written
	 */
	public static void writeShortArray(final DataOutput p_output, final short[] p_array) throws IOException {
		Contract.checkNotNull(p_output, "no output given");
		Contract.checkNotNull(p_array, "no array given");

		p_output.writeInt(p_array.length);
		for (short value : p_array) {
			p_output.writeShort(value);
		}
	}

	/**
	 * Writes a short array
	 * @param p_buffer
	 *            the buffer
	 * @param p_array
	 *            the array
	 */
	public static void writeShortArray(final ByteBuffer p_buffer, final short[] p_array) {
		Contract.checkNotNull(p_buffer, "no buffer given");
		Contract.checkNotNull(p_array, "no array given");

		p_buffer.putInt(p_array.length);
		for (short value : p_array) {
			p_buffer.putShort(value);
		}
	}

	/**
	 * Get the length of multiple bytes
	 * @param p_count
	 *            the number of bytes
	 * @return the length of the bytes
	 */
	public static int getByteArrayWriteLength(final int p_count) {
		return p_count + 4;
	}

	/**
	 * Writes a byte array
	 * @param p_output
	 *            the output
	 * @param p_array
	 *            the array
	 * @throws IOException
	 *             if the array could not be written
	 */
	public static void writeByteArray(final DataOutput p_output, final byte[] p_array) throws IOException {
		Contract.checkNotNull(p_output, "no output given");
		Contract.checkNotNull(p_array, "no array given");

		p_output.writeInt(p_array.length);
		for (byte value : p_array) {
			p_output.writeByte(value);
		}
	}

	/**
	 * Writes a byte array
	 * @param p_buffer
	 *            the buffer
	 * @param p_array
	 *            the array
	 */
	public static void writeByteArray(final ByteBuffer p_buffer, final byte[] p_array) {
		Contract.checkNotNull(p_buffer, "no buffer given");
		Contract.checkNotNull(p_array, "no array given");

		p_buffer.putInt(p_array.length);
		for (byte value : p_array) {
			p_buffer.put(value);
		}
	}

	/**
	 * Get the length of multiple integers
	 * @param p_count
	 *            the number of integers
	 * @return the length of the integers
	 */
	public static int getIntArrayWriteLength(final int p_count) {
		return 4 * p_count + 4;
	}

	/**
	 * Writes a integer array
	 * @param p_output
	 *            the output
	 * @param p_array
	 *            the array
	 * @throws IOException
	 *             if the array could not be written
	 */
	public static void writeIntArray(final DataOutput p_output, final int[] p_array) throws IOException {
		Contract.checkNotNull(p_output, "no output given");
		Contract.checkNotNull(p_array, "no array given");

		p_output.writeInt(p_array.length);
		for (int value : p_array) {
			p_output.writeInt(value);
		}
	}

	/**
	 * Writes a integer array
	 * @param p_buffer
	 *            the buffer
	 * @param p_array
	 *            the array
	 */
	public static void writeIntArray(final ByteBuffer p_buffer, final int[] p_array) {
		Contract.checkNotNull(p_buffer, "no buffer given");
		Contract.checkNotNull(p_array, "no array given");

		p_buffer.putInt(p_array.length);
		for (int value : p_array) {
			p_buffer.putInt(value);
		}
	}

	/**
	 * Get the length of multiple floats
	 * @param p_count
	 *            the number of floats
	 * @return the length of the floats
	 */
	public static int getFloatArrayWriteLength(final int p_count) {
		return 4 * p_count + 4;
	}

	/**
	 * Writes a float array
	 * @param p_output
	 *            the output
	 * @param p_array
	 *            the array
	 * @throws IOException
	 *             if the array could not be written
	 */
	public static void writeFloatArray(final DataOutput p_output, final float[] p_array) throws IOException {
		Contract.checkNotNull(p_output, "no output given");
		Contract.checkNotNull(p_array, "no array given");

		p_output.writeInt(p_array.length);
		for (float value : p_array) {
			p_output.writeFloat(value);
		}
	}

	/**
	 * Writes a float array
	 * @param p_buffer
	 *            the buffer
	 * @param p_array
	 *            the array
	 */
	public static void writeFloatArray(final ByteBuffer p_buffer, final float[] p_array) {
		Contract.checkNotNull(p_buffer, "no buffer given");
		Contract.checkNotNull(p_array, "no array given");

		p_buffer.putInt(p_array.length);
		for (float value : p_array) {
			p_buffer.putFloat(value);
		}
	}

	/**
	 * Get the length of multiple doubles
	 * @param p_count
	 *            the number of doubles
	 * @return the length of the doubles
	 */
	public static int getDoubleArrayWriteLength(final int p_count) {
		return 8 * p_count + 4;
	}

	/**
	 * Writes a double array
	 * @param p_output
	 *            the output
	 * @param p_array
	 *            the array
	 * @throws IOException
	 *             if the array could not be written
	 */
	public static void writeDoubleArray(final DataOutput p_output, final double[] p_array) throws IOException {
		Contract.checkNotNull(p_output, "no output given");
		Contract.checkNotNull(p_array, "no array given");

		p_output.writeInt(p_array.length);
		for (double value : p_array) {
			p_output.writeDouble(value);
		}
	}

	/**
	 * Writes a double array
	 * @param p_buffer
	 *            the buffer
	 * @param p_array
	 *            the array
	 */
	public static void writeDoubleArray(final ByteBuffer p_buffer, final double[] p_array) {
		Contract.checkNotNull(p_buffer, "no buffer given");
		Contract.checkNotNull(p_array, "no array given");

		p_buffer.putInt(p_array.length);
		for (double value : p_array) {
			p_buffer.putDouble(value);
		}
	}

	/**
	 * Get the length of multiple booleans
	 * @param p_count
	 *            the number of booleans
	 * @return the length of the booleans
	 */
	public static int getBooleanArrayWriteLength(final int p_count) {
		return p_count + 4;
	}

	/**
	 * Writes a boolean array
	 * @param p_output
	 *            the output
	 * @param p_array
	 *            the array
	 * @throws IOException
	 *             if the array could not be written
	 */
	public static void writeBooleanArray(final DataOutput p_output, final boolean[] p_array) throws IOException {
		Contract.checkNotNull(p_output, "no output given");
		Contract.checkNotNull(p_array, "no array given");

		p_output.writeInt(p_array.length);
		for (boolean value : p_array) {
			writeBoolean(p_output, value);
		}
	}

	/**
	 * Writes a boolean array
	 * @param p_buffer
	 *            the buffer
	 * @param p_array
	 *            the array
	 */
	public static void writeBooleanArray(final ByteBuffer p_buffer, final boolean[] p_array) {
		Contract.checkNotNull(p_buffer, "no buffer given");
		Contract.checkNotNull(p_array, "no array given");

		p_buffer.putInt(p_array.length);
		for (boolean value : p_array) {
			writeBoolean(p_buffer, value);
		}
	}

}
