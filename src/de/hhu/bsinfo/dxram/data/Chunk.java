
package de.hhu.bsinfo.dxram.data;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Default/generic implementation of a DataStructure. This can be used if the there is no
 * need to further specify the data to be stored as a chunk (i.e. a byte buffer is fine for the job).
 * Furthermore this class is used internally when chunks are moved between different nodes. The actual
 * structure is unknown and not relevant for these tasks, as we just want to work with the payload as
 * one package.
 * If a chunk is requested from the ChunkService, the internal buffer will be adjusted to the
 * actual size of the stored chunk in memory.
 * @author Florian Klein 09.03.2012
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class Chunk implements DataStructure {
	public static final long INVALID_CHUNKID = ChunkID.INVALID_ID;
	public static final int MAX_CHUNK_SIZE = 1024 * 1024 * 16;

	private long m_chunkID = INVALID_CHUNKID;
	private ByteBuffer m_data;

	/**
	 * Constructor
	 * Sets the chunk id to invalid.
	 * @param p_bufferSize
	 *            p_bufferSize Initial size of the byte buffer. If unknown/to read the complete payload
	 *            stored for the specified ID, you can set this to 0. The importObject function will
	 *            allocate the exact size this chunk occupies in memory.
	 */
	public Chunk(final int p_bufferSize) {
		m_chunkID = ChunkID.INVALID_ID;
		m_data = ByteBuffer.allocate(p_bufferSize);
	}

	/**
	 * Constructor
	 * @param p_id
	 *            ID the chunk is assigned to.
	 * @param p_bufferSize
	 *            Initial size of the byte buffer. If unknown/to read the complete payload
	 *            stored for the specified ID, you can set this to 0. The importObject function will
	 *            allocate the exact size this chunk occupies in memory.
	 */
	public Chunk(final long p_id, final int p_bufferSize) {
		m_chunkID = p_id;
		m_data = ByteBuffer.allocate(p_bufferSize);
	}

	/**
	 * Constructor
	 * Sets the chunk id to invalid.
	 * Create the chunk with an external buffer.
	 * @param p_buffer
	 *            External buffer containing the data for the chunk. Be careful
	 *            with shared references of the ByteBuffer object.
	 */
	public Chunk(final ByteBuffer p_buffer) {
		m_chunkID = ChunkID.INVALID_ID;
		m_data = p_buffer;
	}

	/**
	 * Constructor
	 * Create the chunk with an external buffer.
	 * @param p_id
	 *            ID the chunk is assigned to.
	 * @param p_buffer
	 *            External buffer containing the data for the chunk. Be careful
	 *            with shared references of the ByteBuffer object.
	 */
	public Chunk(final long p_id, final ByteBuffer p_buffer) {
		m_chunkID = p_id;
		m_data = p_buffer;
	}

	/**
	 * Constructor
	 * Create the chunk with chunk id, without data.
	 * @param p_id
	 *            ID the chunk is assigned to.
	 */
	public Chunk(final long p_id) {
		m_chunkID = p_id;
	}

	/**
	 * Constructor
	 * Create an empty chunk. Used for pooling.
	 */
	public Chunk() {
		m_chunkID = ChunkID.INVALID_ID;
	}

	/**
	 * Gets the underlying byte buffer with the stored payload.
	 * @note The position gets reseted to 0 before returning the reference.
	 * @return ByteBuffer with position reseted.
	 */
	public final ByteBuffer getData() {
		if (m_data == null) {
			return null;
		} else {
			m_data.position(0);
			return m_data;
		}
	}

	/**
	 * Change the ID of this chunk. This can be used to re-use pre-allocated chunks (pooling).
	 * @param p_id
	 *            New ID to set for this chunk.
	 */
	@Override
	public void setID(final long p_id) {
		m_chunkID = p_id;
	}

	/**
	 * Gets the size of the data/payload.
	 * @return Payload size in bytes.
	 */
	public final int getDataSize() {
		if (m_data == null) {
			return 0;
		} else {
			return m_data.capacity();
		}
	}

	@Override
	public final String toString() {
		return this.getClass().getSimpleName() + "[" + Long.toHexString(m_chunkID) + ", " + getDataSize() + "]";
	}

	@Override
	public long getID() {
		return m_chunkID;
	}

	@Override
	public int importObject(final Importer p_importer, final int p_size) {
		if (m_data == null || m_data.capacity() != p_size) {
			m_data = ByteBuffer.allocate(p_size);
		}
		return p_importer.readBytes(m_data.array());
	}

	@Override
	public int exportObject(final Exporter p_exporter, final int p_size) {
		int size = p_size;

		if (m_data == null) {
			return 0;
		} else {
			if (size > m_data.capacity()) {
				size = m_data.capacity();
			}

			return p_exporter.writeBytes(m_data.array(), 0, size);
		}
	}

	@Override
	public int sizeofObject() {
		return getDataSize();
	}

	@Override
	public boolean hasDynamicObjectSize() {
		return true;
	}
}
