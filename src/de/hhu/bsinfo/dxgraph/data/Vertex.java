package de.hhu.bsinfo.dxgraph.data;

import java.util.Arrays;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Basic vertex object that can be extended with further data if desired.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 09.09.16
 */
public class Vertex implements DataStructure {

	public static final long INVALID_ID = ChunkID.INVALID_ID;

	private long m_id = ChunkID.INVALID_ID;
	private boolean m_neighborsAreEdgeObjects;
	private boolean m_locked;

	private long[] m_neighborIDs = new long[0];

	/**
	 * Constructor
	 */
	public Vertex() {

	}

	/**
	 * Constructor
	 *
	 * @param p_id Chunk id to assign.
	 */
	public Vertex(final long p_id) {
		m_id = p_id;
	}

	// -----------------------------------------------------------------------------

	@Override
	public long getID() {
		return m_id;
	}

	@Override
	public void setID(final long p_id) {
		m_id = p_id;
	}

	/**
	 * Check if the neighbor IDs of this vertex refer to actual edge objects
	 * that can store data.
	 *
	 * @return If true, neighbor IDs refer to actual edge objects, false if
	 * they refer to the neighbor vertex directly.
	 */
	public boolean areNeighborsEdgeObjects() {
		return m_neighborsAreEdgeObjects;
	}

	/**
	 * Set if the neighbor IDs refer to edge objects or directly to the
	 * neighbor vertices.
	 *
	 * @param p_edgeObjects True if refering to edge objects, false to vertex objects.
	 */
	public void setNeighborsAreEdgeObjects(final boolean p_edgeObjects) {
		m_neighborsAreEdgeObjects = p_edgeObjects;
	}

	/**
	 * Check if this vertex was locked.
	 *
	 * @return True if locked, false otherwise.
	 */
	public boolean isLocked() {
		return m_locked;
	}

	/**
	 * Set this vertex locked.
	 *
	 * @param p_locked True for locked, false unlocked.
	 */
	public void setLocked(boolean p_locked) {
		m_locked = p_locked;
	}

	/**
	 * Add a new neighbour to the currently existing list.
	 * This will expand the array by one entry and
	 * add the new neighbour at the end.
	 *
	 * @param p_neighbour Neighbour vertex Id to add.
	 */
	public void addNeighbour(final long p_neighbour) {
		setNeighbourCount(m_neighborIDs.length + 1);
		m_neighborIDs[m_neighborIDs.length - 1] = p_neighbour;
	}

	/**
	 * Get the neighbour array.
	 *
	 * @return Neighbour array with vertex ids.
	 */
	public long[] getNeighbours() {
		return m_neighborIDs;
	}

	/**
	 * Get the number of neighbors of this vertex.
	 *
	 * @return Number of neighbors.
	 */
	public int getNeighborCount() {
		return m_neighborIDs.length;
	}

	/**
	 * Resize the neighbour array.
	 *
	 * @param p_count Number of neighbours to resize to.
	 */
	public void setNeighbourCount(final int p_count) {
		if (p_count != m_neighborIDs.length) {
			// grow or shrink array
			m_neighborIDs = Arrays.copyOf(m_neighborIDs, p_count);
		}
	}

	// -----------------------------------------------------------------------------

	@Override
	public void importObject(final Importer p_importer) {
		byte flags = p_importer.readByte();
		m_neighborsAreEdgeObjects = (flags & (1 << 1)) > 0;
		m_locked = (flags & (1 << 2)) > 0;

		m_neighborIDs = new long[p_importer.readInt()];
		p_importer.readLongs(m_neighborIDs);
	}

	@Override
	public int sizeofObject() {
		int size = 0;

		size += Byte.BYTES;
		size += m_neighborIDs.length * Long.BYTES;
		return size;
	}

	@Override
	public void exportObject(final Exporter p_exporter) {

		byte flags = 0;
		flags |= m_neighborsAreEdgeObjects ? (1 << 0) : 0;
		flags |= m_locked ? (1 << 1) : 0;

		p_exporter.writeByte(flags);

		p_exporter.writeInt(m_neighborIDs.length);
		p_exporter.writeLongs(m_neighborIDs, 0, m_neighborIDs.length);
	}

	@Override
	public String toString() {
		return "Vertex[m_id " + Long.toHexString(m_id)
				+ ", m_neighborsAreEdgeObjects " + m_neighborsAreEdgeObjects
				+ ", m_locked " + m_locked
				+ ", m_neighborsCount " + m_neighborIDs.length
				+ "]: ";
	}
}
