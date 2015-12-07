
package de.uniduesseldorf.dxcompute.data;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.utils.Contract;

/**
 * Stores data
 * @author Florian Klein 09.03.2012
 */
public class ChunkNew implements Comparable<ChunkNew>, DataStructure
{
	// Constants
	public static final long INVALID_CHUNKID = -1;

	// Attributes
	private long m_chunkID;
	private ByteBuffer m_data;

	// Constructors
	public ChunkNew() {
		m_chunkID = INVALID_CHUNKID;
		m_data = null;
	}

	// Getters

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
	public final int compareTo(final ChunkNew p_chunk) {
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
	public void write(final DataStructureWriter p_writer) 
	{
		p_writer.putInt(0, m_data.capacity());
		p_writer.putBytes(4, m_data.array(), 0, m_data.capacity());
	}

	@Override
	public void read(final DataStructureReader p_reader) 
	{
		m_data = ByteBuffer.allocate(p_reader.getInt(0));
		p_reader.getBytes(4, m_data.array(), 0, m_data.capacity());
	}

	@Override
	public int sizeof() 
	{
		return 	8
			+	4
			+ 	m_data.capacity();
	}

	@Override
	public boolean hasDynamicSize() {
		return true;
	}

}
