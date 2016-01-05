
package de.uniduesseldorf.dxram.core.data;

import java.nio.ByteBuffer;

import de.uniduesseldorf.utils.serialization.Exporter;
import de.uniduesseldorf.utils.serialization.Importer;

/**
 * Stores data
 * @author Florian Klein 09.03.2012
 */
public class Chunk implements DataStructure
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

	@Override
	public final String toString() {
		return this.getClass().getSimpleName() + "[" + Long.toHexString(m_chunkID) + ", " + m_data.capacity() + "]";
	}
	
	@Override
	public long getID() {
		return m_chunkID;
	}

	@Override
	public int importObject(final Importer p_importer, final int p_size) {
		m_data = ByteBuffer.allocate(p_size);
		return p_importer.readBytes(m_data.array());
	}
	
	@Override
	public int exportObject(final Exporter p_exporter, final int p_size) {
		int size = p_size;
		
		if (size > m_data.capacity()) {
			size = m_data.capacity();
		} 
		
		return p_exporter.writeBytes(m_data.array(), 0, size);
	}

	@Override
	public int sizeofObject() {
		return m_data.capacity();
	}

	@Override
	public boolean hasDynamicObjectSize() {
		return true;
	}
}
