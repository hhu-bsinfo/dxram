
package de.uniduesseldorf.dxram.core.chunk;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;

import de.uniduesseldorf.utils.Contract;
import de.uniduesseldorf.utils.config.Configuration.ConfigurationConstants;

/**
 * Stores data
 * @author Florian Klein 09.03.2012
 */
public class Chunk implements Comparable<Chunk>, DataStructure
{
	// Constants
	public static final long INVALID_CHUNKID = -1;

	// Attributes
	private long m_chunkID;
	private ByteBuffer m_data;

	// Constructors
	public Chunk() {
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
	public void write(final long p_startAddress, final DataStructureWriter p_writer) 
	{
		p_writer.putInt(p_startAddress, 0, m_data.capacity());
		p_writer.putBytes(p_startAddress, 4, m_data.array(), 0, m_data.capacity());
	}

	@Override
	public void read(final long p_startAddress, final DataStructureReader p_reader)
	{
		m_data = ByteBuffer.allocate(p_reader.getInt(p_startAddress, 0));
		p_reader.getBytes(p_startAddress, 4, m_data.array(), 0, m_data.capacity());
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
