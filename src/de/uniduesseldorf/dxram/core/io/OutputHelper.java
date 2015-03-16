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
 * Writes ChunkID, NodeID, Chunk and OIDTree to DataOutput
 * @author Florian Klein 26.03.2012
 */
public final class OutputHelper {

	// Constructors
	/**
	 * Creates an instance of DXRAMDataOutputHelper
	 */
	private OutputHelper() {}

	// Methods
	public static int getChunkIDWriteLength() {
		return 8;
	}

	/**
	 * Writes a ChunkID to DataOutput
	 * @param p_output
	 *            the DataOutput
	 * @param p_chunkID
	 *            the ChunkID
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

	public static int getChunkIDsWriteLength(int p_count) {
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

	private static byte[] parseOIDTree(final OIDTree p_tree) {
		byte[] ret = null;
		ByteArrayOutputStream byteArrayOutputStream;
		ObjectOutput objectOutput = null;

		byteArrayOutputStream = new ByteArrayOutputStream();
		try {
			objectOutput = new ObjectOutputStream(byteArrayOutputStream);
			objectOutput.writeObject(p_tree);
			ret = byteArrayOutputStream.toByteArray();
		} catch (IOException e) {} finally {
			try {
				if (objectOutput != null) {
					objectOutput.close();
				}
				byteArrayOutputStream.close();
			} catch (final IOException e) {}
		}

		return ret;
	}

	public static int getBooleanWriteLength() {
		return 1;
	}

	public static void writeBoolean(final DataOutput p_output, final boolean p_boolean) throws IOException {
		Contract.checkNotNull(p_output, "no output given");

		p_output.writeBoolean(p_boolean);
	}

	public static void writeBoolean(final ByteBuffer p_buffer, final boolean p_boolean) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		if (p_boolean) {
			p_buffer.put((byte)1);
		} else {
			p_buffer.put((byte)0);
		}
	}

	public static int getIntWriteLength() {
		return 4;
	}

	public static void writeInt(final DataOutput p_output, final int p_int) throws IOException {
		Contract.checkNotNull(p_output, "no output given");

		p_output.writeInt(p_int);
	}

	public static void writeInt(final ByteBuffer p_buffer, final int p_int) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		p_buffer.putInt(p_int);
	}

	public static int getByteWriteLength() {
		return 1;
	}

	public static void writeByte(final DataOutput p_output, final byte p_byte) throws IOException {
		Contract.checkNotNull(p_output, "no output given");

		p_output.writeByte(p_byte);
	}

	public static void writeByte(final ByteBuffer p_buffer, final byte p_byte) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		p_buffer.put(p_byte);
	}

	public static int getShortWriteLength() {
		return 2;
	}

	public static void writeShort(final DataOutput p_output, final short p_short) throws IOException {
		Contract.checkNotNull(p_output, "no output given");

		p_output.writeShort(p_short);
	}

	public static void writeShort(final ByteBuffer p_buffer, final short p_short) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		p_buffer.putShort(p_short);
	}

	public static int getLongWriteLength() {
		return 8;
	}

	public static void writeLong(final DataOutput p_output, final long p_long) throws IOException {
		Contract.checkNotNull(p_output, "no output given");

		p_output.writeLong(p_long);
	}

	public static void writeLong(final ByteBuffer p_buffer, final long p_long) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		p_buffer.putLong(p_long);
	}

	public static int getFloatWriteLength() {
		return 4;
	}

	public static void writeFloat(final DataOutput p_output, final float p_float) throws IOException {
		Contract.checkNotNull(p_output, "no output given");

		p_output.writeFloat(p_float);
	}

	public static void writeFloat(final ByteBuffer p_buffer, final float p_float) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		p_buffer.putFloat(p_float);
	}

	public static int getDoubleWriteLength() {
		return 8;
	}

	public static void writeDouble(final DataOutput p_output, final double p_double) throws IOException {
		Contract.checkNotNull(p_output, "no output given");

		p_output.writeDouble(p_double);
	}

	public static void writeDouble(final ByteBuffer p_buffer, final double p_double) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		p_buffer.putDouble(p_double);
	}

	public static int getLocationsWriteLength() {
		return 24;
	}

	public static void writeLocations(final DataOutput p_output, final Locations p_locations) throws IOException {
		Contract.checkNotNull(p_output, "no output given");
		Contract.checkNotNull(p_locations, "no locations given");

		p_output.writeLong(p_locations.convertToLong());
		p_output.writeLong(p_locations.getStartID());
		p_output.writeLong(p_locations.getEndID());
	}

	public static void writeLocations(final ByteBuffer p_buffer, final Locations p_locations) {
		Contract.checkNotNull(p_buffer, "no buffer given");
		Contract.checkNotNull(p_locations, "no locations given");

		p_buffer.putLong(p_locations.convertToLong());
		p_buffer.putLong(p_locations.getStartID());
		p_buffer.putLong(p_locations.getEndID());
	}

	public static int getLongArrayWriteLength(final int p_count) {
		return 8 * p_count + 4;
	}

	public static void writeLongArray(final DataOutput p_output, final long[] p_array) throws IOException {
		Contract.checkNotNull(p_output, "no output given");
		Contract.checkNotNull(p_array, "no array given");

		p_output.writeInt(p_array.length);
		for (long value : p_array) {
			p_output.writeLong(value);
		}
	}

	public static void writeLongArray(final ByteBuffer p_buffer, final long[] p_array) {
		Contract.checkNotNull(p_buffer, "no buffer given");
		Contract.checkNotNull(p_array, "no array given");

		p_buffer.putInt(p_array.length);
		for (long value : p_array) {
			p_buffer.putLong(value);
		}
	}

	public static int getShortArrayWriteLength(final int p_count) {
		return 2 * p_count + 4;
	}

	public static void writeShortArray(final DataOutput p_output, final short[] p_array) throws IOException {
		Contract.checkNotNull(p_output, "no output given");
		Contract.checkNotNull(p_array, "no array given");

		p_output.writeInt(p_array.length);
		for (short value : p_array) {
			p_output.writeShort(value);
		}
	}

	public static void writeShortArray(final ByteBuffer p_buffer, final short[] p_array) {
		Contract.checkNotNull(p_buffer, "no buffer given");
		Contract.checkNotNull(p_array, "no array given");

		p_buffer.putInt(p_array.length);
		for (short value : p_array) {
			p_buffer.putShort(value);
		}
	}

	public static int getByteArrayWriteLength(final int p_count) {
		return p_count + 4;
	}

	public static void writeByteArray(final DataOutput p_output, final byte[] p_array) throws IOException {
		Contract.checkNotNull(p_output, "no output given");
		Contract.checkNotNull(p_array, "no array given");

		p_output.writeInt(p_array.length);
		for (byte value : p_array) {
			p_output.writeByte(value);
		}
	}

	public static void writeByteArray(final ByteBuffer p_buffer, final byte[] p_array) {
		Contract.checkNotNull(p_buffer, "no buffer given");
		Contract.checkNotNull(p_array, "no array given");

		p_buffer.putInt(p_array.length);
		for (byte value : p_array) {
			p_buffer.put(value);
		}
	}

	public static int getIntArrayWriteLength(final int p_count) {
		return 4 * p_count + 4;
	}

	public static void writeIntArray(final DataOutput p_output, final int[] p_array) throws IOException {
		Contract.checkNotNull(p_output, "no output given");
		Contract.checkNotNull(p_array, "no array given");

		p_output.writeInt(p_array.length);
		for (int value : p_array) {
			p_output.writeInt(value);
		}
	}

	public static void writeIntArray(final ByteBuffer p_buffer, final int[] p_array) {
		Contract.checkNotNull(p_buffer, "no buffer given");
		Contract.checkNotNull(p_array, "no array given");

		p_buffer.putInt(p_array.length);
		for (int value : p_array) {
			p_buffer.putInt(value);
		}
	}

	public static int getFloatArrayWriteLength(final int p_count) {
		return 4 * p_count + 4;
	}

	public static void writeFloatArray(final DataOutput p_output, final float[] p_array) throws IOException {
		Contract.checkNotNull(p_output, "no output given");
		Contract.checkNotNull(p_array, "no array given");

		p_output.writeInt(p_array.length);
		for (float value : p_array) {
			p_output.writeFloat(value);
		}
	}

	public static void writeFloatArray(final ByteBuffer p_buffer, final float[] p_array) {
		Contract.checkNotNull(p_buffer, "no buffer given");
		Contract.checkNotNull(p_array, "no array given");

		p_buffer.putInt(p_array.length);
		for (float value : p_array) {
			p_buffer.putFloat(value);
		}
	}

	public static int getDoubleArrayWriteLength(final int p_count) {
		return 8 * p_count + 4;
	}

	public static void writeDoubleArray(final DataOutput p_output, final double[] p_array) throws IOException {
		Contract.checkNotNull(p_output, "no output given");
		Contract.checkNotNull(p_array, "no array given");

		p_output.writeInt(p_array.length);
		for (double value : p_array) {
			p_output.writeDouble(value);
		}
	}

	public static void writeDoubleArray(final ByteBuffer p_buffer, final double[] p_array) {
		Contract.checkNotNull(p_buffer, "no buffer given");
		Contract.checkNotNull(p_array, "no array given");

		p_buffer.putInt(p_array.length);
		for (double value : p_array) {
			p_buffer.putDouble(value);
		}
	}

	public static int getBooleanArrayWriteLength(final int p_count) {
		return p_count + 4;
	}

	public static void writeBooleanArray(final DataOutput p_output, final boolean[] p_array) throws IOException {
		Contract.checkNotNull(p_output, "no output given");
		Contract.checkNotNull(p_array, "no array given");

		p_output.writeInt(p_array.length);
		for (boolean value : p_array) {
			writeBoolean(p_output, value);
		}
	}

	public static void writeBooleanArray(final ByteBuffer p_buffer, final boolean[] p_array) {
		Contract.checkNotNull(p_buffer, "no buffer given");
		Contract.checkNotNull(p_array, "no array given");

		p_buffer.putInt(p_array.length);
		for (boolean value : p_array) {
			writeBoolean(p_buffer, value);
		}
	}

}
