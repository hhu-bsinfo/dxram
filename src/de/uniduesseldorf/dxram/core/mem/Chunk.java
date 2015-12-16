
package de.uniduesseldorf.dxram.core.mem;

import java.nio.ByteBuffer;

/**
 * Stores data
 * @author Florian Klein 09.03.2012
 */
public class Chunk implements Comparable<Chunk>, DataStructure
{
	// Constants
	public static final long INVALID_CHUNKID = -1;

	// Attributes
	private long m_chunkID = INVALID_CHUNKID;
	private ByteBuffer m_data = null;

	// Constructors
	public Chunk(final long p_id, final int p_bufferSize) {
		m_chunkID = p_id;
		m_data = ByteBuffer.allocate(p_bufferSize);
	}

	/**
	 * Gets the data
	 * @return the data
	 */
	public final ByteBuffer getData() {
		if (m_data != null)
			m_data.position(0);

		return m_data;
	}

	/**
	 * Gets the size of the data
	 * @return the size of the data
	 */
	public final int getDataSize() {
		return m_data.capacity();
	}

	// Methods
	@Override
	public final int compareTo(final Chunk p_chunk) {
		return (int) (m_chunkID - p_chunk.m_chunkID);
	}

	@Override
	public final String toString() {
		return this.getClass().getSimpleName() + "[" + Long.toHexString(m_chunkID) + ", " + m_data.capacity() + "]";
	}
	
	@Override
	public long getID() {
		return m_chunkID;
	}

	@Override
	public int writePayload(final long p_startAddress, final DataStructureWriter p_writer) 
	{
		p_writer.putBytes(p_startAddress, 4, m_data.array(), 0, m_data.capacity());
		return m_data.capacity();
	}

	@Override
	public int readPayload(final long p_startAddress, final int p_dataLength, final DataStructureReader p_reader)
	{
		m_data = ByteBuffer.allocate(p_dataLength);
		p_reader.getBytes(p_startAddress, 4, m_data.array(), 0, m_data.capacity());
		return m_data.capacity();
	}

	@Override
	public int sizeofPayload() 
	{
		return m_data.capacity();
	}
}
