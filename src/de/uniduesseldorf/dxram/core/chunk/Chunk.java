package de.uniduesseldorf.dxram.core.chunk;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.utils.Contract;

/**
 * Stores data
 * @author Florian Klein 09.03.2012
 */
public class Chunk implements Comparable<Chunk> {

	// Constants
	private static final int MAX_SIZE = Core.getConfiguration().getIntValue(ConfigurationConstants.CHUNK_MAXSIZE);

	// Attributes
	private long m_chunkID;
	private int m_version;
	private ByteBuffer m_data;

	// Constructors
	/**
	 * Creates an instance of Chunk
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_localID
	 *            the LocalID
	 * @param p_size
	 *            the size
	 * @param p_version
	 *            the version
	 */
	public Chunk(final short p_nodeID, final long p_localID, final int p_size, final int p_version) {
		this(((long)p_nodeID << 48) + p_localID, p_size);
		m_version = p_version;
	}

	/**
	 * Creates an instance of Chunk
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_localID
	 *            the LocalID
	 * @param p_size
	 *            the size
	 */
	public Chunk(final short p_nodeID, final long p_localID, final int p_size) {
		this(((long)p_nodeID << 48) + p_localID, p_size);
	}

	/**
	 * Creates an instance of Chunk
	 * @param p_chunkID
	 *            the Chunk ID
	 * @param p_size
	 *            the size
	 */
	public Chunk(final long p_chunkID, final int p_size) {
		Contract.check(p_size > 0 && p_size <= MAX_SIZE, "invalid size given");

		m_chunkID = p_chunkID;
		m_version = 0;
		m_data = ByteBuffer.allocate(p_size);
	}

	/**
	 * Creates an instance of Chunk
	 * @param p_chunkID
	 *            the Chunk ID
	 * @param p_data
	 *            the data
	 */
	public Chunk(final long p_chunkID, final byte[] p_data) {
		Contract.check(p_data != null && p_data.length <= MAX_SIZE, "invalid size given");

		m_chunkID = p_chunkID;
		m_version = 0;
		m_data = ByteBuffer.wrap(p_data);
	}

	// Getters
	/**
	 * Gets the ChunkID
	 * @return the ChunkID
	 */
	public final long getChunkID() {
		return m_chunkID;
	}

	/**
	 * Gets the version
	 * @return the version
	 */
	public final int getVersion() {
		return m_version;
	}

	/**
	 * Gets the data
	 * @return the data
	 */
	public final ByteBuffer getData() {
		m_data.position(0);

		return m_data;
	}

	/**
	 * Gets the size of the data
	 * @return the size of the data
	 */
	public final int getSize() {
		return m_data.capacity();
	}

	// Setters
	/**
	 * Sets the ChunkID
	 * @param p_chunkID
	 *            the new ChunkID
	 */
	public final void setChunkID(final long p_chunkID) {
		// This is for testing only! Do not change the ChunkID manually!
		m_chunkID = p_chunkID;
	}

	// Methods
	/**
	 * Increments version
	 */
	public final void incVersion() {
		m_version++;
	}

	@Override
	public final int compareTo(final Chunk p_chunk) {
		return (int)(getChunkID() - p_chunk.getChunkID());
	}

	@Override
	public final String toString() {
		return this.getClass().getSimpleName() + "[" + Long.toHexString(m_chunkID) + ", " + m_data.capacity() + "]";
	}

}
